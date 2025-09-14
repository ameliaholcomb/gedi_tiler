import argparse
import h5py
import geopandas as gpd
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple

import time

from common import schema_parser
from database import ducky
from database.tiles import Tile
from common import s3_utils
from constants import GediProduct


QDEGRADE = [0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68]


def get_cmd_args():
    p = argparse.ArgumentParser(
        description="Generate hierarchical H3 database for fast spatial querying."
    )
    p.add_argument(
        "-s",
        "--schema_path",
        dest="schema_path",
        type=str,
        required=True,
        help="Location of a file containing the database schema",
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

    args.schema = schema_parser.check_schema(args.schema_path)
    args.prefix = args.prefix.strip("/").rstrip("/")

    # Check for a valid TileID
    if args.tile_id:
        try:
            args.tile = Tile(args.tile_id)
        except Exception as e:
            raise ValueError(f"Could not parse tile ID {args.tile_id}: {e}")

    return args


def _get_indices_in_tile(f, beam, columns, tile):
    """Get the range of shot indices for a single beam that lie in the tile."""
    # TODO: This function could use some tests.
    # e.g. individual values can be nan, no data, lons/lats not in order
    lat_col = [c for c in columns.values() if "lat_lowestmode" in c.lower()]
    lon_col = [c for c in columns.values() if "lon_lowestmode" in c.lower()]
    lats = f[f"{beam}/{lat_col[0]}"][:]
    lons = f[f"{beam}/{lon_col[0]}"][:]
    return np.where(
        (lons >= tile.minx)
        & (lons < tile.maxx)
        & (lats > tile.miny)
        & (lats <= tile.maxy)
    )


def load_granule_product(
    rfs: s3_utils.RefreshableFSSpec,
    s3url: str,
    columns: Dict[str, str],
    tile: Tile,
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
    try:
        with rfs.get_fs().open(s3url, mode="rb") as f, h5py.File(f) as hdf5:
            full_df = []
            for k in hdf5.keys():
                if not k.startswith("BEAM"):
                    continue
                idxs = _get_indices_in_tile(hdf5, k, columns, tile)
                if len(idxs) == 0:  # no tile data in beam
                    continue
                dfs = {}
                for j in columns.keys():
                    if "ancillary" in columns[j].lower():
                        anci[j] = hdf5[f"{k}/{columns[j]}"][:][0]
                        continue
                    d = hdf5[f"{k}/{columns[j]}"][idxs]
                    if d.ndim == 2:
                        for col in range(d.shape[-1]):
                            jj = f"{j}_{col:03d}"
                            dfs[jj] = d[:, col]
                    else:
                        dfs[j] = d
                dfs = pd.DataFrame(dfs)
                dfs["beam_name"] = k
                full_df.append(dfs)
    except Exception:
        try:
            # Try again with new credentials, but if that doesn't work, fail.
            print("Refreshing S3 credentials and retrying...")
            rfs.refresh()
            return load_granule_product(rfs, s3url, columns, tile)
        except Exception as e:
            raise e
    if len(full_df) == 0:
        return pd.DataFrame()  # no tile data in granule
    full_df = pd.concat(full_df)
    for j in anci.keys():
        full_df[j] = anci[j]

    return full_df.dropna().set_index("shot_number")


def load_granule(
    rfs: s3_utils.RefreshableFSSpec,
    granule: str,
    product_files: List[Tuple[str, str]],
    schema,
    tile: Tile,
    qf: bool = True,
) -> gpd.GeoDataFrame:
    """Load dataframes for all products and join into a single geodataframe.
    Args:
        granule: Granule name (e.g. OrbitID_GranuleID)
        product_files: List of tuples of the form (product, s3url)
            e.g. [("l4a", "s3://..."), ("l2b", "s3://...")]
        schema: Dictionary defining the data schema for each product.
    """
    dfs = []
    for product, s3url in product_files:
        print("Reading product", product, "from", s3url)
        columns = {
            name: schema[product]["variables"][name]["SDS_Name"]
            for name in schema[product]["variables"].keys()
        }
        df = load_granule_product(rfs, s3url, columns, tile)
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


def load_tile_metadata(tile_id: str, con, bucket: str, prefix: str):
    """Load metadata for a specific tile from S3.
    Args:
        tile_id: Tile ID to load (e.g. N00W000)
        con: DuckDB connection
        bucket: S3 bucket where the metadata is stored.
        prefix: S3 prefix (folder) where the metadata is stored.
    Returns:
        GeoDataFrame with the metadata for the specified tile.
    """
    md_spec = ducky.metadata_spec(bucket, prefix, tile_id)
    md = con.sql(f"""
        SELECT * REPLACE ST_AsText(geometry) AS geometry 
        FROM read_parquet('{md_spec}')
    """).df()
    return gpd.GeoDataFrame(
        md, geometry=gpd.GeoSeries.from_wkt(md["geometry"]), crs="EPSG:4326"
    )


def run_main(args: argparse.Namespace):
    """Main function to create a tile."""
    t1 = time.time()

    # Load metadata for the tile
    con = ducky.init_duckdb()
    granules = load_tile_metadata(args.tile_id, con, args.bucket, args.prefix)
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
                (GediProduct.L2A.value, row.level2A_url),
                (GediProduct.L2B.value, row.level2B_url),
                (GediProduct.L4A.value, row.level4A_url),
                (GediProduct.L4C.value, row.level4C_url),
            ],
            schema=args.schema,
            tile=args.tile,
            qf=args.quality,
        )
        print(f"Loaded {len(df)} shots from granule {row.granule_key}.")
        dfs.append(df)
    full_df = pd.concat(dfs)
    full_df["tile_id"] = args.tile_id
    t3 = time.time()
    print(f"Loading granules took {t3 - t2:.1f} seconds.")

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
