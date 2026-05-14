"""One-shot script to capture per-product CMR query results for the
sabah_box shape over 2021, saved as parquet fixtures. The test for
get_granule_metadata mocks cmr_query.query to return these instead of
hitting the live CMR API.

Run once to (re)build fixtures:
    conda run -n pyduck python gtiler/tests/fixtures/build_cmr_query_fixtures.py
"""

import datetime as dt
import pathlib
import sys

import geopandas as gpd

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from gtiler.common import cmr_query, shape_parser  # noqa: E402
from gtiler.database.schema import GediProduct  # noqa: E402

FIXTURES = pathlib.Path(__file__).parent
SHAPE_PATH = pathlib.Path("/projects/my-public-bucket/shapefiles/sabah_box/box.shp")
START = dt.datetime(2021, 1, 1)
END = dt.datetime(2021, 12, 31, 23, 59, 59)

PRODUCTS = [
    GediProduct.L2A,
    GediProduct.L2B,
    GediProduct.L4A,
    GediProduct.L4C,
]


def main():
    out_dir = FIXTURES / "cmr_query"
    out_dir.mkdir(parents=True, exist_ok=True)

    shp = gpd.read_file(SHAPE_PATH)
    spatial = shape_parser.check_and_format_shape(
        shp, exterior_cw=False, simplify=True
    )

    for product in PRODUCTS:
        print(f"querying CMR for {product.value} ...")
        df = cmr_query.query(
            product,
            spatial=spatial,
            date_range=(START, END),
            use_cloud=True,
        )
        out = out_dir / f"{product.value}.pkl"
        # Pickle (not parquet): cmr_query.query returns a GeoDataFrame
        # with shapely MultiPolygons in granule_poly that isn't the
        # active geometry, which pyarrow can't serialise.
        df.to_pickle(out)
        print(f"  wrote {len(df)} rows to {out}")


if __name__ == "__main__":
    main()
