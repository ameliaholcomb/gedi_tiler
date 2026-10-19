"""Submit DPS jobs rewriting every tile-year file that a migration applies to.

Asks the migration which data files it is pending for, and sends those
to gedi-tile-rewriter jobs, a few files per job. Run it again after jobs
fail: files already rewritten are no longer pending and are left out.
Before submitting, aborts any upload under data/ left unfinished by a
rewrite killed a day or more ago.

Usage:
    python scripts/runners/rewrite_runner.py --bucket maap-ops-workspace \
        --prefix shared/ameliah/tiled_gedi_v3 \
        --migration <module> --algo_version deploy-XXXXXXXX \
        --job_code <job code> -i 1 --submit_interval 2 [--dry_run]
"""

import argparse
import datetime
import importlib
import pathlib
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import pyarrow.fs
from maap.maap import MAAP

from gtiler.common import s3_utils

logger = logging.getLogger(__name__)

# Migrations live in scripts/tools/migrations/.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "tools"))

DATA_KEY = re.compile(r"/data/tile_id=([^/]+)/year=(\d+)/data_0\.parquet$")


def data_files(bucket, prefix):
    """(tile_id, year) -> key for every data file."""
    files = {}
    paginator = boto3.client("s3").get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/data/"):
        for obj in page.get("Contents", []):
            m = DATA_KEY.search(obj["Key"])
            if m:
                files[(m.group(1), int(m.group(2)))] = obj["Key"]
    return files


def pending(migration, bucket, files, workers=32):
    """The (tile_id, year) of each file the migration is pending for."""
    fs = pyarrow.fs.S3FileSystem(region="us-west-2")

    def is_pending(item):
        (tile_id, _), key = item
        return migration.pending(tile_id, f"{bucket}/{key}", fs)

    with ThreadPoolExecutor(workers) as ex:
        return [ty for ty, p in zip(files, ex.map(is_pending, files.items())) if p]


def abort_stale_uploads(bucket, prefix, older_than=datetime.timedelta(days=1)):
    s3 = boto3.client("s3")
    cutoff = datetime.datetime.now(datetime.timezone.utc) - older_than
    paginator = s3.get_paginator("list_multipart_uploads")
    n = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/data/"):
        for u in page.get("Uploads", []):
            if u["Initiated"] < cutoff:
                s3.abort_multipart_upload(Bucket=bucket, Key=u["Key"], UploadId=u["UploadId"])
                n += 1
    logger.info("Aborted %d unfinished uploads from before %s.", n, cutoff)


def main(args):
    migration = importlib.import_module(f"migrations.{args.migration}")
    files = data_files(args.bucket, args.prefix)
    logger.info("Checking which of %d files %s is pending for ...", len(files), args.migration)
    todo = sorted(pending(migration, args.bucket, files))
    jobs = [todo[i : i + args.files_per_job] for i in range(0, len(todo), args.files_per_job)]
    logger.info(
        "Planning %d jobs for %d files (%d per job) on %s.",
        len(jobs), len(todo), args.files_per_job, args.queue,
    )
    if args.dry_run or not jobs:
        return
    abort_stale_uploads(args.bucket, args.prefix)

    maap = s3_utils.call_maap_api(MAAP)
    # Issue in batches of 50; the pace sets how many run at once.
    for i in range(0, len(jobs), 50):
        for job in jobs[i : i + 50]:
            tile_years = ",".join(f"{t}:{y}" for t, y in job)
            logger.info("Submitting job for %s ...", tile_years)
            s3_utils.call_maap_api(
                maap.submitJob,
                identifier=f"rewrite_{args.job_code}_{args.job_iteration}",
                algo_id="gedi-tile-rewriter",
                version=args.algo_version,
                queue=args.queue,
                bucket=args.bucket,
                prefix=args.prefix,
                migration=args.migration,
                tile_years=tile_years,
            )
        if i + 50 < len(jobs):
            time.sleep(args.submit_interval * 60)


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
    p.add_argument("--algo_version", required=True, help="Registered gedi-tile-rewriter version.")
    p.add_argument("--job_code", required=True, help="Tag shared by this run's jobs.")
    p.add_argument("--job_iteration", "-i", type=int, required=True)
    p.add_argument("--files_per_job", type=int, default=20)
    p.add_argument("--queue", default="maap-dps-worker-8gb")
    p.add_argument(
        "--submit_interval",
        type=float,
        required=True,
        help="Minutes to wait between batches of 50 jobs.",
    )
    p.add_argument("--dry_run", action="store_true")
    main(p.parse_args())
