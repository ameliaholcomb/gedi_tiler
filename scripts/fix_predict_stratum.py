"""Rewrite tile-year files whose predict_stratum_l4a column is BLOB, making
it VARCHAR as the builder now writes it. Everything else is copied as is,
in order, with the builder's output settings.

Builds before the schema had types wrote the column's HDF5 byte strings as
BLOB. Each file is rewritten locally, checked, and uploaded on condition
the original is unchanged.

Usage:
    python scripts/fix_predict_stratum.py --bucket maap-ops-workspace \
        --prefix shared/ameliah/tiled_gedi_v3 [--workers 8] [--dry_run]
"""

import argparse
import logging
import os
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor

import boto3

from gtiler.common import s3_utils
from gtiler.database import ducky

logger = logging.getLogger(__name__)

COLUMN = "predict_stratum_l4a"
# The builder's output settings (scripts/dps_tile_builder.py).
ROW_GROUP_SIZE = 200_000


def data_keys(bucket, prefix):
    paginator = boto3.client("s3").get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/data/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                yield obj["Key"]


def column_type(con, url):
    return con.sql(f"""
        SELECT column_type FROM (
            DESCRIBE SELECT {COLUMN}
            FROM read_parquet('{url}', hive_partitioning = false)
        )
    """).fetchone()[0]


def fix(bucket, key, dry_run):
    """Rewrite one file if its column is BLOB. Returns what was done."""
    con = ducky.init_duckdb()
    con.execute("SET threads = 1;")
    con.execute("SET preserve_insertion_order = true;")
    url = f"s3://{bucket}/{key}"
    kind = column_type(con, url)
    if kind == "VARCHAR":
        return "already VARCHAR"
    if kind != "BLOB":
        raise TypeError(f"{key}: {COLUMN} is {kind}")
    if dry_run:
        return "would fix"
    etag = s3_utils.object_etag(bucket, key)
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, "fixed.parquet")
        con.sql(f"""
            COPY (
                SELECT * REPLACE (decode({COLUMN}) AS {COLUMN})
                FROM read_parquet('{url}', hive_partitioning = false)
            ) TO '{local}' (
                FORMAT parquet,
                GEOPARQUET_VERSION 'V2',
                COMPRESSION zstd,
                ROW_GROUP_SIZE {ROW_GROUP_SIZE}
            );
        """)
        # Same rows, in the same order, with only the column's type changed.
        mismatched = con.sql(f"""
            SELECT count(*) FROM (
                SELECT row_number() OVER () AS r, shot_number,
                       decode({COLUMN}) AS s
                FROM read_parquet('{url}', hive_partitioning = false)
            ) a FULL JOIN (
                SELECT row_number() OVER () AS r, shot_number, {COLUMN} AS s
                FROM read_parquet('{local}')
            ) b USING (r)
            WHERE a.shot_number IS DISTINCT FROM b.shot_number
               OR a.s IS DISTINCT FROM b.s
        """).fetchone()[0]
        if mismatched:
            raise RuntimeError(f"{key}: {mismatched} rows differ after rewrite")
        with open(local, "rb") as f:
            s3_utils.conditional_multipart_put(bucket, key, f, if_match=etag)
    return "fixed"


def main(args):
    keys = list(data_keys(args.bucket, args.prefix))
    logger.info("Checking %d files ...", len(keys))
    counts = {}
    with ProcessPoolExecutor(args.workers) as ex:
        futures = {
            ex.submit(fix, args.bucket, key, args.dry_run): key for key in keys
        }
        for future, key in futures.items():
            result = future.result()
            counts[result] = counts.get(result, 0) + 1
            logger.info("%s: %s", key, result)
    logger.info("Done: %s", counts)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", required=True)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--dry_run", action="store_true")
    main(p.parse_args())
