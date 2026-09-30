"""Mark predict_stratum_l4a as a string in tile-year files that store it as
BLOB, making it VARCHAR as the builder now writes it.

Builds before the schema had types wrote the column's HDF5 byte strings as
BLOB. The data are the same either way (BYTE_ARRAY); only the footer's
schema entry differs, by converted_type UTF8. So each file's footer is
patched, and the object is rebuilt on S3 from a server-side copy of its
data pages and the new footer, on condition the original is unchanged.

Usage:
    python scripts/fix_predict_stratum.py --bucket maap-ops-workspace \
        --prefix shared/ameliah/tiled_gedi_v3 [--workers 32] [--dry_run]
"""

import argparse
import collections
import logging
import struct
import sys
from concurrent.futures import ThreadPoolExecutor

import boto3

from gtiler.common import thrift_compact as tc
from gtiler.database import ducky

logger = logging.getLogger(__name__)

COLUMN = "predict_stratum_l4a"
# parquet FileMetaData and SchemaElement fields, and the UTF8 converted type
SCHEMA, NUM_ROWS = 2, 3
NAME, CONVERTED_TYPE, UTF8 = 4, 6, 0
# S3 needs every part but the last to be at least 5 MB, and at most 5 GB.
MIN_PART, MAX_PART = 5 * 2**20, 5 * 2**30

s3 = boto3.client("s3")


def data_objects(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/data/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                yield obj["Key"], obj["Size"], obj["ETag"]


def get_range(bucket, key, etag, start, end):
    return s3.get_object(
        Bucket=bucket, Key=key, Range=f"bytes={start}-{end - 1}", IfMatch=etag
    )["Body"].read()


def read_footer(bucket, key, size, etag):
    """The footer's bytes and where they start."""
    tail = get_range(bucket, key, etag, size - 8, size)
    if tail[4:] != b"PAR1":
        raise ValueError(f"{key} is not a parquet file")
    start = size - 8 - struct.unpack("<I", tail[:4])[0]
    return get_range(bucket, key, etag, start, size - 8), start


def patch_footer(key, footer):
    """The footer with the column marked UTF8 (None if it already is), and
    the file's row count."""
    metadata, end = tc.decode(footer)
    if end != len(footer) or tc.encode(metadata) != footer:
        raise ValueError(f"{key}: footer does not round-trip")
    num_rows = tc.field(metadata, NUM_ROWS)
    (element,) = [
        e for e in tc.field(metadata, SCHEMA)[1]
        if tc.field(e, NAME) == COLUMN.encode()
    ]
    kind = tc.field(element, CONVERTED_TYPE)
    if kind == UTF8:
        return None, num_rows
    if kind is not None:
        raise TypeError(f"{key}: {COLUMN} has converted type {kind}")
    tc.set_field(element, CONVERTED_TYPE, tc.I32, UTF8)
    return tc.encode(metadata), num_rows


def rewrite(bucket, key, etag, data_end, tail):
    """Replace the object with its bytes before data_end, then tail,
    copying the former on the server side."""
    upload_id = s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
    try:
        parts, start = [], 0
        if data_end < MIN_PART:  # too small for a copied first part
            tail = get_range(bucket, key, etag, 0, data_end) + tail
            start = data_end
        while start < data_end:
            end = min(start + MAX_PART, data_end)
            r = s3.upload_part_copy(
                Bucket=bucket, Key=key, UploadId=upload_id,
                PartNumber=len(parts) + 1,
                CopySource={"Bucket": bucket, "Key": key},
                CopySourceRange=f"bytes={start}-{end - 1}",
                CopySourceIfMatch=etag,
            )
            parts.append(
                {"PartNumber": len(parts) + 1, "ETag": r["CopyPartResult"]["ETag"]}
            )
            start = end
        r = s3.upload_part(
            Bucket=bucket, Key=key, UploadId=upload_id,
            PartNumber=len(parts) + 1, Body=tail,
        )
        parts.append({"PartNumber": len(parts) + 1, "ETag": r["ETag"]})
        s3.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id,
            MultipartUpload={"Parts": parts}, IfMatch=etag,
        )
    except Exception:
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise


def check(bucket, key, num_rows):
    """Read the patched column in full; check its type and row count."""
    con = ducky.init_duckdb()
    url = f"s3://{bucket}/{key}"
    kind = con.sql(
        f"SELECT column_type FROM (DESCRIBE SELECT {COLUMN} FROM '{url}')"
    ).fetchone()[0]
    n, _ = con.sql(f"SELECT count(*), max({COLUMN}) FROM '{url}'").fetchone()
    if kind != "VARCHAR" or n != num_rows:
        raise RuntimeError(f"{key}: {COLUMN} is {kind} with {n} of {num_rows} rows")


def fix(bucket, key, size, etag, dry_run):
    """Patch one file if its column is BLOB. Returns what was done."""
    footer, start = read_footer(bucket, key, size, etag)
    new, num_rows = patch_footer(key, footer)
    if new is None:
        return "already VARCHAR"
    if dry_run:
        return "would fix"
    rewrite(bucket, key, etag, start, new + struct.pack("<I", len(new)) + b"PAR1")
    check(bucket, key, num_rows)
    return "fixed"


def main(args):
    objects = list(data_objects(args.bucket, args.prefix))
    logger.info("Checking %d files ...", len(objects))
    counts = collections.Counter()
    with ThreadPoolExecutor(args.workers) as ex:
        futures = [
            (key, ex.submit(fix, args.bucket, key, size, etag, args.dry_run))
            for key, size, etag in objects
        ]
        for i, (key, future) in enumerate(futures, 1):
            counts[future.result()] += 1
            if i % 500 == 0:
                logger.info("%d files: %s", i, dict(counts))
    logger.info("Done: %s", dict(counts))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", required=True)
    p.add_argument("--workers", type=int, default=32)
    p.add_argument("--dry_run", action="store_true")
    main(p.parse_args())
