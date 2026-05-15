import argparse
import boto3
import geopandas as gpd
import logging
import pandas as pd
import sys
import time
from maap.maap import MAAP

from gtiler.common import shape_parser, s3_utils
from gtiler.common.granule_metadata import get_granule_metadata
from gtiler.database import ducky, tiles
from gtiler.database.schema import GediProduct

logger = logging.getLogger(__name__)

# Map the lowercase CLI flag tokens for --required_products to the
# matching GediProduct enum values.
PRODUCT_FLAG_NAMES = {
    "l2a": GediProduct.L2A,
    "l2b": GediProduct.L2B,
    "l4a": GediProduct.L4A,
    "l4c": GediProduct.L4C,
}


def parse_required_products(raw: str) -> list:
    """Parse a comma-separated `--required_products` value into a list of
    GediProduct enums, validating that every token is a known product."""
    tokens = [t.strip().lower() for t in raw.split(",") if t.strip()]
    if not tokens:
        raise argparse.ArgumentTypeError(
            "--required_products must list at least one product"
        )
    unknown = [t for t in tokens if t not in PRODUCT_FLAG_NAMES]
    if unknown:
        raise argparse.ArgumentTypeError(
            f"unknown required_products: {unknown}; "
            f"valid: {sorted(PRODUCT_FLAG_NAMES)}"
        )
    # Preserve order, dedupe.
    seen = []
    for t in tokens:
        p = PRODUCT_FLAG_NAMES[t]
        if p not in seen:
            seen.append(p)
    return seen

def get_queue(tile_id):
    if (("N47" in tile_id) |
        ("S47" in tile_id) |
        ("N48" in tile_id) |
        ("S48" in tile_id) |
        ("N49" in tile_id) |
        ("S49" in tile_id) |
        ("N50" in tile_id) | 
        ("S50" in tile_id) |
        ("N51" in tile_id) |
        ("S51" in tile_id)):
        return "maap-dps-worker-16gb"
    if (("N52" in tile_id) |
        ("S52" in tile_id)):
        return "maap-dps-worker-32gb"
    else:
        return "maap-dps-worker-8gb"

def get_tile_ids_novalidation(bucket, prefix):
    prefix = f"{prefix}/data/"
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    tile_ids = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter="/",          # stop at the next path segment
    ):
        for cp in page.get("CommonPrefixes", []):
            # cp["Prefix"] looks like: "path/to/data/tile_id=N06_W123/"
            segment = cp["Prefix"].rstrip("/").split("/")[-1]  # "tile_id=N06_W123"
            if segment.startswith("tile_id="):
                tile_ids.append(segment.split("=", 1)[1])     # "N06_W123"
    return tile_ids
    
def main(args):
    # Metadata is written first, then data is backfilled by the DPS jobs.
    # DPS jobs can fail, be re-run, etc, but the metadata is only written once.

    # If metadata was written erroneously, search for metadata tiles with the
    # problematic tile_id or cmr_access_time and delete them.

    # This script generates the metadata describing the per-tile granule info for the region,
    # (which is also where each DPS tile builder looks to find which granules to process),
    # and updates S3://<db>/metadata/ with any tiles not already present in S3://<db>/metadata/.
    # It then submits a job for each tile in the region that does not already exist in S3://<db>/data/.

    # 1. Get required tiles for region
    logger.info("Determining required tiles for region...")
    covering_tiles, covering = tiles.get_covering_tiles_for_region(args.shape)
    products = [
        GediProduct.L2A,
        GediProduct.L2B,
        GediProduct.L4A,
        GediProduct.L4C,
    ]
    # Get CMR metadata for all granules covering the region. We query
    # every product unconditionally so that non-required products still
    # populate when CMR has them; granules that lack any required product
    # are dropped, and the rest get NaN URLs for their missing products.
    logger.info(
        "Required products: %s",
        [p.value for p in args.required_products],
    )
    cmr_md = get_granule_metadata(
        shape_parser.check_and_format_shape(
            gpd.GeoDataFrame(geometry=covering),
            exterior_cw=False,
            simplify=True,
        ),
        products,
        start_year=args.start_year,
        end_year=args.end_year,
        required_products=args.required_products,
    )
    BAD_GRANULES = ["O33765_03"]
    cmr_md = cmr_md[~cmr_md.granule_key.isin(BAD_GRANULES)]
    
    # Save the geometry column so that it will not be dropped in the sjoin
    cmr_md["granule_geometry"] = cmr_md.geometry
    # Join to find which granules are needed for each tile
    tile_granule_gdf = covering_tiles.sjoin(
        cmr_md, how="inner", predicate="intersects"
    )
    tile_granule_gdf.drop(columns=["index_right"], inplace=True)
    tile_granule_gdf["cmr_access_time"] = pd.Timestamp.now(tz="UTC")
    required_tiles = set(tile_granule_gdf.tile_id.unique())

    # 2. Get existing metadata tiles in S3
    con = ducky.init_duckdb()
    logger.info("Scanning existing metadata ...")
    path = ducky.metadata_prefix(args.bucket, args.prefix)
    if s3_utils.s3_prefix_exists(path):
        md_spec = ducky.metadata_spec(args.bucket, args.prefix)
        existing_md = con.execute(
            f"SELECT DISTINCT tile_id FROM read_parquet('{md_spec}')"
        ).fetchall()
        existing_md = {x[0] for x in existing_md}

        tile_granule_gdf = tile_granule_gdf[
            ~tile_granule_gdf.tile_id.isin(existing_md)
        ]
    else:
        existing_md = set()

    # 3. Get existing tiles in the database
    # check if the database path exists:
    logger.info("Checking for existing tiles in the database...")
    path = ducky.data_prefix(args.bucket, args.prefix)
    if s3_utils.s3_prefix_exists(path):
        if args.fast_scan:
            existing_tiles = set(get_tile_ids_novalidation(args.bucket, args.prefix))
            logger.info("Found %d existing tiles (fast scan).", len(existing_tiles))
        else:
            data_spec = ducky.data_spec(args.bucket, args.prefix)
            existing_tiles = con.execute(
                f"SELECT DISTINCT tile_id FROM read_parquet('{data_spec}')"
            ).fetchall()
            existing_tiles = {x[0] for x in existing_tiles}
            # TODO: Do we need a way for jobs to mark that they completed but had no data?
    else:
        existing_tiles = set()

    # tiles with data but no metadata:
    wrong = [x for x in existing_tiles if x not in existing_md]
    if len(wrong) > 0:
        logger.warning(
            "Warning: %d tiles have data but no metadata."
            " Please delete these tiles from the database before continuing: %s",
            len(wrong),
            ", ".join(wrong),
        )
        exit(1)

    missing_tiles = [x for x in required_tiles if x not in existing_tiles]
    relevant_md_tiles = {x for x in existing_md if x in required_tiles}
    relevant_data_tiles = {x for x in existing_tiles if x in required_tiles}
    logger.info("%d tiles in the region.", len(required_tiles))
    logger.info("%d metadata tiles in the database for this region.", len(relevant_md_tiles))
    logger.info("%d tiles already exist in the database for this region.", len(relevant_data_tiles))
    logger.info("Planning to add metadata for %d new tiles.", len(required_tiles) - len(relevant_md_tiles))
    logger.info("(Which should match this number: %d)", tile_granule_gdf.tile_id.nunique())
    logger.info("Planning to create jobs to process data for %d tiles.", len(missing_tiles))

    if args.dry_run:
        return

    # 3. Create new metadata dataframe for tiles in region and write to S3
    # the metadata that we expect to describe the database after all jobs complete
    if len(tile_granule_gdf) > 0:
        if not args.no_confirm:
            input("To proceed to create tile metadata, press ENTER >>>")
        logger.info("Writing metadata for required tiles to S3...")

        ducky.gdf_to_duck(
            con,
            tile_granule_gdf,
            "tile_granule_gdf",
            geometry_columns=["geometry", "granule_geometry"],
        )
        md_prefix = ducky.metadata_prefix(args.bucket, args.prefix)
        con.sql(f"""
            COPY tile_granule_gdf TO '{md_prefix}' (
                FORMAT parquet,
                PARTITION_BY ({ducky.TILE_ID}),
                COMPRESSION zstd,
                OVERWRITE_OR_IGNORE
            );
        """)

        logfile = f"logs/tile_plan_{args.job_code}_{args.job_iteration}.txt"
        with open(logfile, "w") as f:
            for tile_id in sorted(missing_tiles):
                f.write(f"{tile_id}\n")
        logger.info("Proposed metadata for tiles listed in %s written to database.", logfile)
    if not args.no_confirm:
        input("To proceed to create jobs, press ENTER >>>")

    # 4. Submit jobs for tiles in required_tiles but not in existing_tiles
    maap = MAAP()
    # too many tasks result in quota limits on DAAC S3 reads
    max_tasks = 900
    # issue in batches of 50 every 5 minutes.
    for i in range(0, len(missing_tiles), 50):
        batch = missing_tiles[i : i + 50]
        for tile_id in batch:
            logger.info("Submitting job for tile %s...", tile_id)
            job_name = f"tiler_{args.job_code}_{args.job_iteration}"
            queue = get_queue(tile_id)
            job = maap.submitJob(
                identifier=job_name,
                algo_id="gedi-tile-writer",
                version="amelia-deploy-k1rCo7To",
                # version="amelia-deploy-yfpetMPn",
                queue=queue,
                bucket=args.bucket,
                prefix=args.prefix,
                tile_id=tile_id,
                generation=args.job_iteration,
                checkpoint_interval=25,
                quality="quality",
            )
        if i >= max_tasks:
            return
        time.sleep(5 * 60)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )
    parser = argparse.ArgumentParser(
        description="Manage MAAP jobs to create a tiled GEDI database."
    )
    parser.add_argument(
        "--job_code",
        type=str,
        required=True,
        help="Shared code for all MAAP tasks in this database build (subregion identifier).",
    )
    parser.add_argument(
        "--job_iteration",
        "-i",
        type=int,
        required=True,
        help="Iteration number for this run of the job code.",
    )
    parser.add_argument(
        "--shapefile",
        type=str,
        required=True,
        help="Path to region shapefile to process.",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        required=True,
        help="S3 bucket containing tiled GEDI database.",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        required=True,
        help="S3 prefix for tiled GEDI database.",
    )
    parser.add_argument(
        "--start_year",
        type=int,
        help="Start year for data to include (inclusive).",
    )
    parser.add_argument(
        "--end_year",
        type=int,
        help="End year for data to include (inclusive).",
    )

    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Print execution plan, but do not run any MAAP jobs.",
    )
    parser.add_argument(
        "--no_confirm",
        "-y",
        action="store_true",
        help="Skip confirmation of work plan before writing metadata and creating jobs.",
    )
    parser.add_argument(
        "--fast_scan",
        action="store_true",
        help="Quickly scan existing tiles in database without checking for valid parquet files.",
    )
    parser.add_argument(
        "--required_products",
        type=parse_required_products,
        default=list(PRODUCT_FLAG_NAMES.values()),
        help=(
            "Comma-separated subset of {l2a,l2b,l4a,l4c} (default: all "
            "four). Granules must have every listed product to be "
            "included; missing non-required products are NaN-filled in "
            "the per-tile metadata, and dps_tile_builder NaN-fills their "
            "columns at tile-build time."
        ),
    )

    args = parser.parse_args()
    args.prefix = args.prefix.strip("/").rstrip("/")
    shp = gpd.read_file(args.shapefile)
    args.shape = shp.head(1)
    args.shape = shp

    main(args)
