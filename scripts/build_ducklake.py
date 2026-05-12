import argparse
import logging
import boto3
import tqdm

from gtiler.database import ducky


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


def main():
    args = get_cmd_args()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    file_list = get_file_list(args.bucket, args.prefix)
    logger.info(f"Found {len(file_list)} files to process.")

    tempdir = f"s3://{args.bucket}/{args.prefix}/temp_duckdb/"
    con = ducky.init_duckdb(temp_dir=tempdir)

    # The DATA_PATH stores no data and can be deleted after load
    # because the data lives in the existing parquet files.
    con.sql(f"""--sql
            ATTACH 'ducklake:gedi.ducklake' AS gedi_dl (
            DATA_PATH '{tempdir}');
    """)
    # Build schema from the first file. Geometry is excluded for now because
    # ducklake only supports GeoParquet V2, but the current files are V1.
    con.sql(f"""--sql
            CREATE OR REPLACE TABLE gedi_dl.data AS
            SELECT * EXCLUDE (geometry)
            FROM read_parquet('{file_list[0]}')
            WITH NO DATA;
    """)

    for file in tqdm.tqdm(file_list, desc="Loading parquet files"):
        # fmt:off
        con.execute(f"CALL ducklake_add_data_files('gedi_dl', 'data', '{file}', ignore_extra_columns => true);")
        # fmt:on

    logger.info(f"Committing DuckLake database to S3 ...")
    dest_key = f"{args.prefix}/ducklake/gedi.ducklake"
    s3 = boto3.client("s3")
    s3.upload_file("gedi.ducklake", args.bucket, dest_key)


if __name__ == "__main__":
    main()
