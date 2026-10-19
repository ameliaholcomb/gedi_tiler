import argparse
import logging
import os

import boto3
import tqdm

from gtiler.database import ducky

DUCKLAKE_FILE = "gedi.ducklake"
NEVER_NAN_COLUMNS = ["lat_lowestmode", "lon_lowestmode"]
# One batch per 10-degree latitude band, e.g. N3* covers N30_* to N39_*.
LATITUDE_BAND_GLOBS = [f"{ns}{d}*" for ns in "NS" for d in range(6)]


def get_cmd_args():
    p = argparse.ArgumentParser(
        description="Build a fresh DuckLake catalog of the tiled GEDI parquet "
        "files and upload it over the existing one."
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
        "--ducklake-file",
        type=str,
        default=DUCKLAKE_FILE,
        help="Catalog file name under <prefix>/ducklake/. Use a different "
        "name for test builds so they do not overwrite the main catalog.",
    )
    p.add_argument(
        "--tile-globs",
        type=str,
        nargs="+",
        default=LATITUDE_BAND_GLOBS,
        help="Glob patterns over tile ids, registered one batch per pattern. "
        "Every pattern must match at least one file. Defaults to the whole "
        "database in 10-degree latitude bands.",
    )
    p.add_argument(
        "--threads",
        type=int,
        default=64,
        help="DuckDB threads. Registration is bound by S3 latency on footer "
        "reads, so more threads than cores helps.",
    )
    return p.parse_args()


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


def main():
    args = get_cmd_args()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    data_root = ducky.data_prefix(args.bucket, args.prefix)
    tempdir = f"s3://{args.bucket}/{args.prefix}/temp_duckdb/"
    for f in [args.ducklake_file, f"{args.ducklake_file}.wal"]:
        if os.path.exists(f):
            os.remove(f)

    con = ducky.init_duckdb(temp_dir=tempdir)
    con.execute("SET enable_progress_bar = false;")
    con.execute(f"SET threads = {args.threads};")
    con.sql(f"""--sql
            ATTACH 'ducklake:{args.ducklake_file}' AS gedi_dl (
            DATA_PATH '{tempdir}');
    """)

    # Hive partitioning adds tile_id and year as columns.
    first_file = con.sql(f"""--sql
            SELECT file FROM glob('{data_root}tile_id=*/year=*/*.parquet') LIMIT 1
    """).fetchone()[0]
    con.sql(f"""--sql
            CREATE TABLE gedi_dl.data AS
            SELECT *
            FROM read_parquet('{first_file}', hive_partitioning = true)
            WITH NO DATA;
    """)

    # Register by glob rather than by file list: DuckDB reads the footers of
    # globbed files in parallel, but those of a list one at a time.
    for tile_glob in tqdm.tqdm(args.tile_globs, desc="Batches"):
        pattern = f"{data_root}tile_id={tile_glob}/year=*/*.parquet"
        # fmt:off
        con.execute(f"CALL ducklake_add_data_files('gedi_dl', 'data', '{pattern}', hive_partitioning => true, ignore_extra_columns => true);")
        # fmt:on
        n_files = con.sql(
            "SELECT count(*) FROM __ducklake_metadata_gedi_dl.ducklake_data_file"
        ).fetchone()[0]
        logger.info(f"Registered {tile_glob}: {n_files} files in total.")

    mark_never_nan(con)
    con.close()

    key = f"{args.prefix}/ducklake/{args.ducklake_file}"
    boto3.client("s3").upload_file(args.ducklake_file, args.bucket, key)
    logger.info(f"Uploaded catalog to s3://{args.bucket}/{key}.")


if __name__ == "__main__":
    main()
