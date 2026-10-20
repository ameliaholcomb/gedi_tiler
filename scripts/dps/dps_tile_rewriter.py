"""Rewrite existing tile-year files with a migration, on DPS.

A migration is a module in scripts/tools/migrations/ with
pending(tile_id, path, filesystem=None), rewrite(con, source, output) and
check(con, source, output). For each tile-year, this downloads the file,
and if the migration is pending for it, rewrites it, checks the result,
and uploads it on condition the original is unchanged. Files no longer
pending are skipped, so a failed job can simply be run again.

Usage:
    python scripts/dps/dps_tile_rewriter.py --bucket maap-ops-workspace \
        --prefix shared/ameliah/tiled_gedi_v3 \
        --migration <module in scripts/tools/migrations/> \
        --tile_years N02_W050:2023,N02_W050:2024
"""

import argparse
import importlib
import pathlib
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time

import boto3

from gtiler.common import s3_utils
from gtiler.database import ducky

logger = logging.getLogger(__name__)

# Migrations live in scripts/tools/migrations/.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "tools"))

# The same budget as the tile builder's final write.
DUCKDB_MEMORY_LIMIT = "4GB"
DUCKDB_THREADS = 1


def rewrite_tile_year(con, migration, bucket, prefix, tile_id, year, work_dir):
    key = f"{prefix}/data/tile_id={tile_id}/year={year}/data_0.parquet"
    etag = s3_utils.object_etag(bucket, key)
    source = os.path.join(work_dir, "source.parquet")
    output = os.path.join(work_dir, "output.parquet")
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key, IfMatch=etag)["Body"]
    with open(source, "wb") as f:
        shutil.copyfileobj(body, f, 16 * 2**20)
    if not migration.pending(tile_id, source):
        logger.info("%s needs no rewrite.", key)
        return "skipped"
    t = time.time()
    migration.rewrite(con, source, output)
    migration.check(con, source, output)
    if migration.pending(tile_id, output):
        raise ValueError(f"The rewrite of {key} is still pending")
    with open(output, "rb") as f:
        s3_utils.conditional_multipart_put(bucket, key, f, if_match=etag)
    logger.info(
        "Rewrote %s (%.0f MB -> %.0f MB) in %.0f s.",
        key, os.path.getsize(source) / 1e6, os.path.getsize(output) / 1e6,
        time.time() - t,
    )
    os.remove(source)
    os.remove(output)
    return "rewritten"


def main(args):
    commit = subprocess.check_output(["git", "-C", os.path.dirname(__file__), "rev-parse", "HEAD"], text=True)
    logger.info("Running commit %s", commit.strip())
    migration = importlib.import_module(f"migrations.{args.migration}")
    tile_years = [ty.split(":") for ty in args.tile_years.split(",")]
    # Scratch files live under the working directory, outside the DPS
    # output directory.
    with tempfile.TemporaryDirectory(dir=".", prefix="gtiler_") as work_dir:
        con = ducky.init_duckdb(temp_dir=os.path.join(work_dir, "duckdb"))
        con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}';")
        con.execute(f"SET threads = {DUCKDB_THREADS};")
        results = [
            rewrite_tile_year(
                con, migration, args.bucket, args.prefix, tile_id, int(year), work_dir
            )
            for tile_id, year in tile_years
        ]
    logger.info(
        "Done: %d rewritten, %d skipped.",
        results.count("rewritten"), results.count("skipped"),
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", required=True)
    p.add_argument("--migration", required=True, help="Module name in scripts/tools/migrations/.")
    p.add_argument(
        "--tile_years",
        required=True,
        help="Comma-separated tile_id:year pairs, e.g. N02_W050:2023,N02_W050:2024.",
    )
    main(p.parse_args())
