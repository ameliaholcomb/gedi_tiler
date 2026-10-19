import argparse
import logging
import os

import boto3
import botocore
import tqdm

from gtiler.database import ducky

DUCKLAKE_FILE = "gedi.ducklake"
NEVER_NAN_COLUMNS = ["lat_lowestmode", "lon_lowestmode"]


def get_cmd_args():
    p = argparse.ArgumentParser(
        description="Generate hierarchical H3 database for fast spatial querying."
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


def _checkpoint_key(prefix: str) -> str:
    return f"{prefix}/ducklake/{DUCKLAKE_FILE}"


def open_connection(tempdir: str):
    con = ducky.init_duckdb(temp_dir=tempdir)
    con.sql(f"""--sql
            ATTACH 'ducklake:{DUCKLAKE_FILE}' AS gedi_dl (
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


def download_checkpoint(s3, bucket: str, prefix: str) -> bool:
    """Download an existing ducklake checkpoint from S3. Returns False if none exists."""
    if os.path.exists(DUCKLAKE_FILE):
        os.remove(DUCKLAKE_FILE)
    try:
        s3.download_file(bucket, _checkpoint_key(prefix), DUCKLAKE_FILE)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise
    return True


def upload_checkpoint(s3, bucket: str, prefix: str) -> None:
    s3.upload_file(DUCKLAKE_FILE, bucket, _checkpoint_key(prefix))


def main():
    args = get_cmd_args()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    s3 = boto3.client("s3")

    file_list = get_file_list(args.bucket, args.prefix)
    logger.info(f"Found {len(file_list)} files in source bucket.")

    resuming = download_checkpoint(s3, args.bucket, args.prefix)
    tempdir = f"s3://{args.bucket}/{args.prefix}/temp_duckdb/"

    done: set[str] = set()
    if resuming:
        con = open_connection(tempdir)
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

    # On a fresh run only, build the schema from the first file. Geometry is
    # excluded because ducklake requires GeoParquet V2 and source files are V1.
    if not resuming:
        con = open_connection(tempdir)
        con.sql(f"""--sql
                CREATE OR REPLACE TABLE gedi_dl.data AS
                SELECT * EXCLUDE (geometry)
                FROM read_parquet('{remaining[0]}')
                WITH NO DATA;
        """)
        con.close()

    processed = 0
    for start in tqdm.tqdm(
        range(0, len(remaining), args.batch_size), desc="Batches"
    ):
        batch = remaining[start : start + args.batch_size]
        con = open_connection(tempdir)
        for file in batch:
            # fmt:off
            con.execute(f"CALL ducklake_add_data_files('gedi_dl', 'data', '{file}', ignore_extra_columns => true);")
            # fmt:on
        mark_never_nan(con)
        con.close()
        upload_checkpoint(s3, args.bucket, args.prefix)
        processed += len(batch)
        logger.info(
            f"Checkpointed {len(done) + processed}/{len(file_list)} files."
        )

    logger.info("All files loaded.")


if __name__ == "__main__":
    main()
