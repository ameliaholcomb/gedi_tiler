import argparse
import logging
import os
import re

import boto3
import botocore
import tqdm
from shapely.geometry import box

from gtiler.database import ducky
from gtiler.database.tiles import Tile

DUCKLAKE_FILE = "gedi.ducklake"
NEVER_NAN_COLUMNS = ["lat_lowestmode", "lon_lowestmode"]


def get_cmd_args():
    p = argparse.ArgumentParser(
        description="Register the tiled GEDI parquet files in a DuckLake catalog."
    )
    p.add_argument(
        "-b", "--bucket", type=str, required=True, help="S3 database bucket"
    )
    p.add_argument(
        "-p",
        "--prefix",
        type=str,
        required=True,
        help="S3 prefix (folder) where the database is stored",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of files to register per batch before checkpointing to S3.",
    )
    p.add_argument(
        "--ducklake-file",
        type=str,
        default=DUCKLAKE_FILE,
        help="Catalog file name under <prefix>/ducklake/. Use a different "
        "name for test builds so they do not overwrite the main catalog.",
    )
    p.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("XMIN", "YMIN", "XMAX", "YMAX"),
        help="Only register tiles intersecting this lon/lat box.",
    )
    return p.parse_args()


def get_file_list(bucket: str, prefix: str) -> list[str]:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    key_prefix = f"{prefix}/data/tile_id="
    pages = paginator.paginate(Bucket=bucket, Prefix=key_prefix)
    return [
        f"s3://{bucket}/{obj['Key']}"
        for page in pages
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet") and "/year=" in obj["Key"]
    ]


def filter_to_bbox(files: list[str], bbox: list[float]) -> list[str]:
    region = box(*bbox)
    return [
        f
        for f in files
        if Tile(re.search(r"tile_id=([^/]+)/", f).group(1)).shape.intersects(
            region
        )
    ]


def _checkpoint_key(prefix: str, ducklake_file: str) -> str:
    return f"{prefix}/ducklake/{ducklake_file}"


def open_connection(tempdir: str, ducklake_file: str):
    con = ducky.init_duckdb(temp_dir=tempdir)
    con.sql(f"""--sql
            ATTACH 'ducklake:{ducklake_file}' AS gedi_dl (
            DATA_PATH '{tempdir}');
    """)
    return con


def load_registered_files(con) -> set[str]:
    """Return the set of parquet paths already registered in the attached ducklake."""
    rows = con.sql(
        "SELECT DISTINCT path FROM __ducklake_metadata_gedi_dl.ducklake_data_file"
    ).fetchall()
    return {row[0] for row in rows}


def mark_never_nan(con) -> None:
    """Record that the shot location columns never contain NaN.

    ducklake_add_data_files leaves contains_nan unknown, and DuckLake will
    not prune files on min/max stats of a float column unless it knows the
    column has no NaNs. Lat/lon are always valid, so this lets bounding-box
    filters on them skip files from the catalog alone.
    """
    con.sql(f"""--sql
            UPDATE __ducklake_metadata_gedi_dl.ducklake_file_column_stats
            SET contains_nan = false
            WHERE contains_nan IS NULL AND column_id IN (
                SELECT column_id
                FROM __ducklake_metadata_gedi_dl.ducklake_column
                WHERE column_name IN ({", ".join(f"'{c}'" for c in NEVER_NAN_COLUMNS)})
            );
    """)


def download_checkpoint(s3, bucket: str, prefix: str, ducklake_file: str) -> bool:
    """Download an existing ducklake checkpoint from S3. Returns False if none exists."""
    if os.path.exists(ducklake_file):
        os.remove(ducklake_file)
    try:
        s3.download_file(
            bucket, _checkpoint_key(prefix, ducklake_file), ducklake_file
        )
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise
    return True


def upload_checkpoint(s3, bucket: str, prefix: str, ducklake_file: str) -> None:
    s3.upload_file(
        ducklake_file, bucket, _checkpoint_key(prefix, ducklake_file)
    )


def main():
    args = get_cmd_args()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    s3 = boto3.client("s3")

    file_list = get_file_list(args.bucket, args.prefix)
    logger.info(f"Found {len(file_list)} files in source bucket.")
    if args.bbox:
        file_list = filter_to_bbox(file_list, args.bbox)
        logger.info(f"{len(file_list)} files in bbox {args.bbox}.")

    resuming = download_checkpoint(
        s3, args.bucket, args.prefix, args.ducklake_file
    )
    tempdir = f"s3://{args.bucket}/{args.prefix}/temp_duckdb/"

    done: set[str] = set()
    if resuming:
        con = open_connection(tempdir, args.ducklake_file)
        done = load_registered_files(con)
        con.close()
        logger.info(
            f"Resuming from checkpoint: {len(done)} files already loaded."
        )

    remaining = [f for f in file_list if f not in done]
    logger.info(f"{len(remaining)} files remaining to process.")
    if not remaining:
        logger.info("Nothing to do.")
        return

    # On a fresh run only, build the schema from the first file. Hive
    # partitioning adds tile_id and year as columns.
    if not resuming:
        con = open_connection(tempdir, args.ducklake_file)
        con.sql(f"""--sql
                CREATE OR REPLACE TABLE gedi_dl.data AS
                SELECT *
                FROM read_parquet('{remaining[0]}', hive_partitioning = true)
                WITH NO DATA;
        """)
        con.close()

    processed = 0
    for start in tqdm.tqdm(
        range(0, len(remaining), args.batch_size), desc="Batches"
    ):
        batch = remaining[start : start + args.batch_size]
        con = open_connection(tempdir, args.ducklake_file)
        # One transaction per batch, so the catalog gets one snapshot per
        # batch rather than one per file.
        con.execute("BEGIN TRANSACTION;")
        for file in batch:
            # fmt:off
            con.execute(f"CALL ducklake_add_data_files('gedi_dl', 'data', '{file}', hive_partitioning => true, ignore_extra_columns => true);")
            # fmt:on
        con.execute("COMMIT;")
        # DuckLake writes the new files' stats at commit, so this must follow it.
        mark_never_nan(con)
        con.close()
        upload_checkpoint(s3, args.bucket, args.prefix, args.ducklake_file)
        processed += len(batch)
        logger.info(
            f"Checkpointed {len(done) + processed}/{len(file_list)} files."
        )

    logger.info("All files loaded.")


if __name__ == "__main__":
    main()
