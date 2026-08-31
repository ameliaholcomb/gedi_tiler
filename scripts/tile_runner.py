import argparse
import boto3
import fsspec
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

def _tile_year(path):
    """Pull the (tile_id, year) pair out of a partitioned data path."""
    parts = dict(p.split("=", 1) for p in path.split("/") if "=" in p)
    return parts[ducky.TILE_ID], int(parts[ducky.YEAR])


def get_tile_years_novalidation(bucket, prefix):
    """(tile_id, year) pairs present in the database, from an S3 listing.
    Includes tile-years holding only an empty marker."""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    tile_years = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/data/"):
        for obj in page.get("Contents", []):
            tile_years.add(_tile_year(obj["Key"]))
    return tile_years


def get_empty_tile_years(bucket, prefix):
    """(tile_id, year) pairs whose jobs completed with no footprints."""
    fs = fsspec.filesystem("s3")
    return {
        _tile_year(p) for p in fs.glob(ducky.empty_marker_spec(bucket, prefix))
    }


def required_tile_years(granules, start_year, end_year):
    """(tile_id, year) pairs the granules cover, restricted to the years
    this run was asked to build. Clamping matters: a granule spanning New
    Year would otherwise create a sliver partition for a year that a later
    run would then skip as already present."""
    tile_years = set()
    for tile, start, end in zip(
        granules.tile_id, granules.time_start, granules.time_end
    ):
        for year in range(start.year, end.year + 1):
            if start_year is not None and year < start_year:
                continue
            if end_year is not None and year > end_year:
                continue
            tile_years.add((tile, year))
    return tile_years


def check_quality_consistency(con, md_spec: str, quality: bool):
    """Exit if existing metadata was built with a different quality
    filtering setting than this run, which would leave the database
    inconsistently filtered."""
    mismatched = con.execute(f"""
        SELECT DISTINCT tile_id FROM read_parquet('{md_spec}')
        WHERE quality_filter IS DISTINCT FROM {quality}
    """).fetchall()
    if mismatched:
        tile_ids = sorted(x[0] for x in mismatched)
        logger.warning(
            "%d existing metadata tiles were built with a different quality "
            "filtering setting than --quality=%s: %s%s",
            len(tile_ids),
            quality,
            ", ".join(tile_ids[:10]),
            " ..." if len(tile_ids) > 10 else "",
        )
        exit(1)


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
    tile_granule_gdf["quality_filter"] = args.quality
    required_tiles = set(tile_granule_gdf.tile_id.unique())
    required = required_tile_years(
        tile_granule_gdf, args.start_year, args.end_year
    )

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
        if not args.fast_scan:
            check_quality_consistency(con, md_spec, args.quality)

        tile_granule_gdf = tile_granule_gdf[
            ~tile_granule_gdf.tile_id.isin(existing_md)
        ]
    else:
        existing_md = set()

    # 3. Get existing tile-years in the database
    # check if the database path exists:
    logger.info("Checking for existing tile-years in the database...")
    path = ducky.data_prefix(args.bucket, args.prefix)
    if s3_utils.s3_prefix_exists(path):
        if args.fast_scan:
            existing = get_tile_years_novalidation(args.bucket, args.prefix)
            logger.info("Found %d existing tile-years (fast scan).", len(existing))
        else:
            data_spec = ducky.data_spec(args.bucket, args.prefix)
            existing = con.execute(
                f"SELECT DISTINCT tile_id, year FROM read_parquet('{data_spec}')"
            ).fetchall()
            existing = {(t, int(y)) for t, y in existing}
            # Empty tile-years hold no rows for the scan above to find.
            existing |= get_empty_tile_years(args.bucket, args.prefix)
    else:
        existing = set()

    # tiles with data but no metadata:
    wrong = sorted({t for t, _ in existing if t not in existing_md})
    if len(wrong) > 0:
        logger.warning(
            "Warning: %d tiles have data but no metadata."
            " Please delete these tiles from the database before continuing: %s",
            len(wrong),
            ", ".join(wrong),
        )
        exit(1)

    missing = sorted(required - existing)
    relevant_md_tiles = {x for x in existing_md if x in required_tiles}
    relevant_data = {x for x in existing if x in required}
    logger.info("%d tiles (%d tile-years) in the region.", len(required_tiles), len(required))
    logger.info("%d metadata tiles in the database for this region.", len(relevant_md_tiles))
    logger.info("%d tile-years already exist in the database for this region.", len(relevant_data))
    logger.info("Planning to add metadata for %d new tiles.", len(required_tiles) - len(relevant_md_tiles))
    logger.info("(Which should match this number: %d)", tile_granule_gdf.tile_id.nunique())
    logger.info("Planning to create jobs to process data for %d tile-years.", len(missing))

    if args.dry_run:
        return

    # 3. Create new metadata dataframe for tiles in region and write to S3
    # the metadata that we expect to describe the database after all jobs complete
    if len(tile_granule_gdf) > 0:
        if not args.no_confirm:
            input("To proceed to create tile metadata, press ENTER >>>")
        logger.info("Writing metadata for required tiles to S3...")

        new_md = ducky.gdf_to_duck(
            con,
            tile_granule_gdf,
            geometry_columns=["geometry", "granule_geometry"],
        )
        md_prefix = ducky.metadata_prefix(args.bucket, args.prefix)
        con.sql(f"""
            COPY new_md TO '{md_prefix}' (
                FORMAT parquet,
                PARTITION_BY ({ducky.TILE_ID}),
                COMPRESSION zstd,
                OVERWRITE_OR_IGNORE
            );
        """)

        logfile = f"logs/tile_plan_{args.job_code}_{args.job_iteration}.txt"
        with open(logfile, "w") as f:
            for tile_id, year in missing:
                f.write(f"{tile_id} {year}\n")
        logger.info("Proposed metadata for tiles listed in %s written to database.", logfile)
    if not args.no_confirm:
        input("To proceed to create jobs, press ENTER >>>")

    # 4. Submit jobs for required tile-years not already in the database
    maap = MAAP()
    # too many tasks result in quota limits on DAAC S3 reads
    max_tasks = 900
    # issue in batches of 50 every 5 minutes.
    for i in range(0, len(missing), 50):
        batch = missing[i : i + 50]
        for tile_id, year in batch:
            logger.info("Submitting job for tile %s year %d...", tile_id, year)
            job_name = f"tiler_{args.job_code}_{args.job_iteration}"
            queue = get_queue(tile_id)
            job = maap.submitJob(
                identifier=job_name,
                algo_id="gedi-tile-writer",
                version="amelia-deploy-nScOUwBm",
                # version="amelia-deploy-yfpetMPn",
                queue=queue,
                bucket=args.bucket,
                prefix=args.prefix,
                tile_id=tile_id,
                year=year,
                generation=args.job_iteration,
                checkpoint_interval=25,
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
        help=(
            "Quickly scan existing tiles in database without checking for valid "
            "parquet files, and skip the quality filtering consistency check."
        ),
    )
    parser.add_argument(
        "--quality",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Apply GEDI quality filters to shots as tiles are built. Recorded "
            "per tile in the metadata and read from there by dps_tile_builder."
        ),
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
