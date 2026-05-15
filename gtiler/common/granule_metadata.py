import datetime as dt
import hashlib
import geopandas as gpd
import logging
import pandas as pd

from typing import List, Optional

from gtiler.common import cmr_query, granule_name
from gtiler.database.schema import GediProduct

logger = logging.getLogger(__name__)


def get_granule_key_for_filename(filename: str) -> str:
    parsed = granule_name.parse_granule_filename(filename)
    return f"{parsed.orbit}_{parsed.sub_orbit_granule}"


def hash_string_list(string_list: list) -> str:
    joined = ",".join([f"{len(item)}:{item}" for item in string_list])
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def get_granule_metadata(
    shape: gpd.GeoSeries,
    products: List[GediProduct],
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    required_products: Optional[List[GediProduct]] = None,
) -> gpd.GeoDataFrame:
    """Query CMR for every product in `products` and join the results by
    granule_key. A granule is included if every product in
    `required_products` (defaults to `products`) is present for it;
    non-required products that are absent come through as NaN URLs."""
    if required_products is None:
        required_products = products
    extra = [p for p in required_products if p not in products]
    if extra:
        raise ValueError(
            f"required_products must be a subset of products; "
            f"unexpected: {[p.value for p in extra]}"
        )

    md_list = []
    for product in products:
        logger.info("Querying NASA metadata API for product: %s", product.value)
        date_range = None
        if start_year is not None and end_year is not None:
            date_range = (
                dt.datetime(start_year, 1, 1),
                dt.datetime(end_year, 12, 31, 23, 59, 59),
            )
        df = cmr_query.query(
            product, spatial=shape, date_range=date_range, use_cloud=True
        )
        df["granule_key"] = df.granule_name.map(get_granule_key_for_filename)
        df["product"] = product.value
        df.rename(columns={"granule_url": f"{product.value}_url"}, inplace=True)
        logger.info("Found %d granules for product %s.", len(df), product.value)
        md_list.append(df)
    md = gpd.GeoDataFrame(
        pd.concat(md_list), geometry="granule_poly"
    ).reset_index(drop=True)

    # Keep only granules that have every required product. Non-required
    # products that are absent for a granule come through as NaN URLs in
    # the per-product columns after the groupby below.
    required_set = {p.value for p in required_products}
    present = md.groupby("granule_key")["product"].agg(set)
    omit = present[~present.apply(lambda s: required_set <= s)].index
    logger.info(
        "Excluding %d/%d granules missing one or more required products (%s).",
        len(omit),
        len(present),
        sorted(required_set),
    )
    md = md[~md.granule_key.isin(omit)].reset_index(drop=True)
    md.drop(columns=["product"], inplace=True)
    md = md.groupby(["granule_key"]).agg(
        {
            "granule_size": "sum",
            "granule_name": lambda x: list(x),
            GediProduct.L2A.value + "_url": "first",
            GediProduct.L2B.value + "_url": "first",
            GediProduct.L4A.value + "_url": "first",
            GediProduct.L4C.value + "_url": "first",
            "geometry": "first",
        }
    )
    md.rename(columns={"granule_name": "granule_names"}, inplace=True)
    md["granule_hash"] = md.granule_names.apply(hash_string_list)
    return gpd.GeoDataFrame(
        md, geometry="geometry", crs="EPSG:4326"
    ).reset_index()
