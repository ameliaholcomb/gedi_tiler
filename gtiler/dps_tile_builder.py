import argparse
import h5py
import geopandas as gpd
import numpy as np
import pandas as pd
from typing import List, Tuple

import time

from database import ducky
from database.tiles import Tile
from common import s3_utils
from database.schema import SCHEMA
from database.schema import Product, GeometryColumn  # typing only


QDEGRADE = [0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68]


def get_cmd_args():
    p = argparse.ArgumentParser(
        description="Generate hierarchical H3 database for fast spatial querying."
    )
    p.add_argument(
        "-b",
        "--bucket",
        dest="bucket",
        type=str,
        required=True,
        default=None,
        help="S3 bucket in which to write the output files.",
    )
    p.add_argument(
        "-p",
        "--prefix",
        dest="prefix",
        type=str,
        required=True,
        default=None,
        help="S3 prefix (folder) in which to write the output files.",
    )
    p.add_argument(
        "-tile_id",
        "--tile_id",
        dest="tile_id",
        type=str,
        required=True,
        default=None,
        help=(
            "Tile ID to process. 1ºx1º degree tiles in the format"
            "[N/S][DD][E/W][DDD], defining the coordinates of the"
            "top-left corner of the tile."
        ),
    )
    p.add_argument(
        "-c",
        "--check",
        dest="check",
        action="store_true",
        help="Only check for updates without writing any files.",
    )
    p.add_argument(
        "-test",
        "--test",
        dest="test",
        action="store_true",
        help="Quick test running over only 2 GEDI granules.",
    )
    p.add_argument(
        "-q",
        "--quality",
        dest="quality",
        action="store_true",
        help="Apply quality filters to the data.",
    )
    cmdargs = p.parse_args()
    return cmdargs


def check_args(args: argparse.Namespace) -> argparse.Namespace:
    """Check the command line arguments and return the updated args."""

    args.prefix = args.prefix.strip("/").rstrip("/")

    # Check for a valid TileID
    if args.tile_id:
        try:
            args.tile = Tile(args.tile_id)
        except Exception as e:
            raise ValueError(f"Could not parse tile ID {args.tile_id}: {e}")

    return args


def _get_indices_in_tile(f, beam, geometry: GeometryColumn, tile):
    """Get the range of shot indices for a single beam that lie in the tile."""
    # TODO: This function could use some tests.
    # e.g. individual values can be nan, no data, lons/lats not in order
    lats = f[f"{beam}/{geometry.lat.SDS_Name}"][:]
    lons = f[f"{beam}/{geometry.lon.SDS_Name}"][:]
    return np.where(
        (lons >= tile.minx)
        & (lons < tile.maxx)
        & (lats > tile.miny)
        & (lats <= tile.maxy)
    )


def load_granule_product(
    rfs: s3_utils.RefreshableFSSpec,
    s3url: str,
    product: Product,
    tile: Tile,
    retry_count: int = 1,
) -> pd.DataFrame:
    """Load a GEDI HDF5 file and return a flattened dataframe.
    Args:
        s3url: S3 URL to the GEDI HDF5 file.
        columns: Dictionary of the form {df_name: sds_name},
            defining the columns to extract from the file.
            df_name is the desired output column name,
            sds_name is the full field in the HDF5 file (including group,
                but omitting BEAM).
            e.g. {"lat_lowestmode": "geolocation/lat_lowestmode"}
    """
    anci = {}
    extra = [product.primary_key, product.geometry.lat, product.geometry.lon]
    try:
        with rfs.get_fs().open(s3url, mode="rb") as f, h5py.File(f) as hdf5:
            full_df = []
            for k in hdf5.keys():
                if not k.startswith("BEAM"):
                    continue
                idxs = _get_indices_in_tile(hdf5, k, product.geometry, tile)
                if len(idxs) == 0:  # no tile data in beam
                    continue
                dfs = {}
                for v in product.variables + extra:
                    if "ancillary" in v.SDS_Name.lower():
                        anci[v.variable] = hdf5[f"{k}/{v.SDS_Name}"][:][0]
                        continue
                    d = hdf5[f"{k}/{v.SDS_Name}"][idxs]
                    if d.ndim == 2:
                        # unroll profile data into separate columns
                        for col in range(d.shape[-1]):
                            vv = f"{v.variable}_{col}"
                            dfs[vv] = d[:, col]
                    else:
                        dfs[v.variable] = d
                dfs = pd.DataFrame(dfs)
                dfs["beam_name"] = k
                full_df.append(dfs)
    except Exception as e:
        if retry_count <= 0:
            raise e
        # Try again with new credentials, but if that doesn't work, fail.
        print("Refreshing S3 credentials and retrying...")
        rfs.refresh()
        return load_granule_product(rfs, s3url, product, tile, retry_count - 1)
    if len(full_df) == 0:
        return pd.DataFrame()  # no tile data in granule
    full_df = pd.concat(full_df)
    for j in anci.keys():
        full_df[j] = anci[j]

    return full_df.dropna().set_index("shot_number")


def load_granule(
    rfs: s3_utils.RefreshableFSSpec,
    granule: str,
    product_files: List[Tuple[Product, str]],
    tile: Tile,
    qf: bool = True,
) -> gpd.GeoDataFrame:
    """Load dataframes for all products and join into a single geodataframe.
    Args:
        granule: Granule name (e.g. OrbitID_GranuleID)
        product_files: List of tuples of the form (product, s3url)
            e.g. [("level4A", "s3://..."), ("level2B", "s3://...")]
        schema: Dictionary defining the data schema for each product.
    """
    dfs = []
    for product_schema, s3url in product_files:
        print("Reading product", product_schema.product_level, "from", s3url)
        df = load_granule_product(rfs, s3url, product_schema, tile)
        dfs.append(df)
    full_df = dfs[0]
    for df in dfs[1:]:
        # expected repeated cols -- keep from first product only
        df.drop(
            columns=["beam_name", "lon_lowestmode", "lat_lowestmode"],
            inplace=True,
        )  # expected repeated cols -- keep from first product only
        full_df = full_df.join(df, how="inner")

    # Add derived data columns
    full_df["granule"] = granule
    gedi_count_start = pd.to_datetime("2018-01-01T00:00:00Z")
    full_df["absolute_time"] = gedi_count_start + pd.to_timedelta(
        full_df["delta_time"], "seconds"
    )
    if qf:
        full_df = full_df[
            (full_df["quality_flag"] == 1)
            & (full_df["sensitivity"] >= 0.9)
            & (full_df["sensitivity"] <= 1.0)
            & (full_df["sensitivity_a2"] > 0.95)
            & (full_df["sensitivity_a2"] <= 1.0)
            & (full_df["degrade_flag"].isin(QDEGRADE))
            & (full_df["surface_flag"] == 1)
        ]
    # make shot_number a column now that the join is finished
    full_df.reset_index(inplace=True)
    return full_df


def load_tile_metadata(tile_id: str, bucket: str, prefix: str):
    """Load metadata for a specific tile from S3.
    Args:
        tile_id: Tile ID to load (e.g. N00W000)
        bucket: S3 bucket where the metadata is stored.
        prefix: S3 prefix (folder) where the metadata is stored.
    Returns:
        GeoDataFrame with the metadata for the specified tile.
    """
    # Don't have DuckDB scan the metadata table -- many jobs in parallel
    # may be looking at this table, so just read the file directly.
    md_spec = ducky.metadata_spec(bucket, prefix, tile_id)
    md_spec = md_spec.replace("*", "data_0")
    return gpd.read_file(md_spec)


def run_main(args: argparse.Namespace):
    """Main function to create a tile."""
    t1 = time.time()

    # Load metadata for the tile
    print("Reading metadata for tile ...")
    granules = load_tile_metadata(args.tile_id, args.bucket, args.prefix)
    t2 = time.time()
    print(f"Loading metadata took {t2 - t1:.1f} seconds.")
    if args.test:
        tot = len(granules)
        granules = granules.head(2)
        print(f"Testing mode: using {len(granules)}/{tot} granules.")

    # Set up access to the ORNL and LP DAACs
    rfs = s3_utils.RefreshableFSSpec("/iam/maap-data-reader")

    dfs = []
    for row in granules.itertuples():
        print(f"Loading granule {row.granule_key} ...")
        df = load_granule(
            rfs=rfs,
            granule=row.granule_key,
            product_files=[
                (SCHEMA.products[0], row.level2A_url),
                (SCHEMA.products[1], row.level2B_url),
                (SCHEMA.products[2], row.level4A_url),
                (SCHEMA.products[3], row.level4C_url),
            ],
            tile=args.tile,
            qf=args.quality,
        )
        print(f"Loaded {len(df)} shots from granule {row.granule_key}.")
        dfs.append(df)
    full_df = pd.concat(dfs)
    full_df["tile_id"] = args.tile_id
    t3 = time.time()
    print(f"Loading granules took {t3 - t2:.1f} seconds.")

    con = ducky.init_duckdb()
    aws_prefix = ducky.data_prefix(args.bucket, args.prefix)
    df = con.sql("""
        SELECT *,
            ST_Point(lon_lowestmode, lat_lowestmode) AS geometry,
            date_part('year', absolute_time) AS year
        FROM full_df
    """)
    con.sql(f"""
        COPY df TO '{aws_prefix}' (
            FORMAT parquet,
            PARTITION_BY ({ducky.TILE_ID}, {ducky.YEAR}),
            COMPRESSION zstd,
            ROW_GROUP_SIZE 10_000,
            OVERWRITE_OR_IGNORE
        );
    """)

    t4 = time.time()
    print(f"Writing parquet took {t4 - t3:.1f} seconds.")
    print(f"Total time: {t4 - t1:.1f} seconds.")

    return 0


if __name__ == "__main__":
    args = get_cmd_args()
    args = check_args(args)
    run_main(args)
