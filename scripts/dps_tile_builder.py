import argparse
from botocore.exceptions import ReadTimeoutError, ConnectTimeoutError
import h5py
import geopandas as gpd
import logging
import numpy as np
import pandas as pd
import psutil
import sys
from typing import List, Tuple

import time

from gtiler.database import ducky
from gtiler.database.tiles import Tile
from gtiler.common import s3_utils
from gtiler.common import checkpoint_lib
from gtiler.database.schema import SCHEMA
from gtiler.database.schema import Product, GeometryColumn  # typing only

logger = logging.getLogger(__name__)


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
        "-g",
        "--generation",
        dest="generation",
        type=int,
        default=0,
        help=(
            "Generation number for this job. Used for optimistic concurrency"
            "control of checkpoints. Increment this number to start a new "
            "generation of checkpoints, which will cause older jobs to fail in "
            "favor of the new generation. If the generation number is not "
            "incremented, jobs issued for the same tile will simply win based on "
            "which writes to the checkpoint first."
        ),
    )
    p.add_argument(
        "-i",
        "--checkpoint_interval",
        dest="checkpoint_interval",
        type=int,
        default=30,
        help="Number of granules to process between writing checkpoints.",
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
    retry_count: int = 3,
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
    except (ReadTimeoutError, ConnectTimeoutError) as e:
        logger.warning(f"Timeout reading {s3url}: {e}")
        if retry_count <= 0:
            logger.error(
                f"Timeout reading {s3url} after all retries, giving up."
            )
            raise
        wait = 4 ** (3 - retry_count)  # 4s, 16s, 64s backoff
        logger.warning(
            f"Timeout reading {s3url}, retrying in {wait}s ({retry_count} attempts left)..."
        )
        time.sleep(wait)
        return load_granule_product(rfs, s3url, product, tile, retry_count - 1)
    except Exception as e:
        if retry_count <= 0:
            raise e
        # Try again with new credentials, but if that doesn't work, fail.
        logger.warning("Refreshing S3 credentials and retrying...")
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
        logger.debug(
            "Reading product %s from %s", product_schema.product_level, s3url
        )
        df = load_granule_product(rfs, s3url, product_schema, tile)
        if len(df) == 0:
            return pd.DataFrame({})
        dfs.append(df)
    full_df = dfs[0]
    for df in dfs[1:]:
        # expected repeated cols -- keep from first product only
        df.drop(
            columns=["beam_name", "lon_lowestmode", "lat_lowestmode"],
            inplace=True,
        )  # expected repeated cols -- keep from first product only
        full_df = full_df.join(df, how="inner")
    log_memory(logger, "load_granule after join")

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
    md_spec = ducky.metadata_spec(bucket, prefix, tile_id)
    md_spec = md_spec.replace("*", "data_0")
    return gpd.read_file(md_spec)


def log_memory(logger, message=""):
    """Log the current memory usage."""
    mem_usage_gb = psutil.Process().memory_info().rss / 1024**3
    logger.info(f"Current memory usage: {mem_usage_gb:.2f} GB {message}")


def run_main(args: argparse.Namespace):
    """Main function to create a tile."""
    t1 = time.time()

    # Load metadata for the tile
    logger.info("Reading metadata and checkpoints for tile ...")
    checkpointer = checkpoint_lib.Checkpointer(
        args.bucket, args.prefix, args.tile_id, generation=args.generation
    )
    initial_checkpoint = checkpointer.initialize()
    if initial_checkpoint is None:
        logger.info("Loading new work plan from metadata ...")
        granules_to_process = load_tile_metadata(
            args.tile_id, args.bucket, args.prefix
        )
        processed_data = pd.DataFrame()
    else:
        granules_to_process, processed_data = initial_checkpoint
    if args.test:
        tot = len(granules_to_process)
        granules_to_process = granules_to_process.head(2)
        logger.info(
            "Testing mode: using %d/%d granules.", len(granules_to_process), tot
        )

    t2 = time.time()
    logger.info("%d shots already processed.", len(processed_data))
    logger.info(
        "Planning to process %d new granules.", len(granules_to_process)
    )
    logger.info("Loading metadata and checkpoints took %.1f seconds.", t2 - t1)

    # Set up access to the ORNL and LP DAACs
    rfs = s3_utils.RefreshableFSSpec("/iam/maap-data-reader")

    dfs = [processed_data]
    batch_size = args.checkpoint_interval
    for i in range(0, len(granules_to_process), batch_size):
        batch = granules_to_process[i : i + batch_size]
        for row in batch.itertuples():
            logger.info("Loading granule %s ...", row.granule_key)
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
            logger.info(f"Loaded {len(df)} shots in granule {row.granule_key}")
            dfs.append(df)
        log_memory(logger, "after processing batch")
        checkpointer.write_checkpoint(
            granules_to_process=granules_to_process.iloc[i + batch_size :],
            processed_data=pd.concat(dfs),
        )
    full_df = pd.concat(dfs)
    full_df["tile_id"] = args.tile_id
    t3 = time.time()
    logger.info("Loading granules took %.1f seconds.", t3 - t2)

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
    logger.info("Writing parquet took %.1f seconds.", t4 - t3)
    logger.info("Total time: %.1f seconds.", t4 - t1)

    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )
    args = get_cmd_args()
    args = check_args(args)
    run_main(args)
