"""Check that every tile-year file in a database has the same schema.

Reads each file's parquet footer and groups the files by their column names
and types (ignoring file metadata, such as each file's GeoParquet bbox).
Prints one line per distinct schema, with its file count and an example
file, and the columns on which the schemas differ.

Usage:
    python scripts/check_tile_schemas.py --bucket maap-ops-workspace \
        --prefix shared/ameliah/tiled_gedi_v3 [--workers 32]
"""

import argparse
import collections
from concurrent.futures import ThreadPoolExecutor

import boto3
import pyarrow.fs
import pyarrow.parquet as pq


def data_keys(bucket, prefix):
    paginator = boto3.client("s3").get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/data/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                yield obj["Key"]


def main(args):
    fs = pyarrow.fs.S3FileSystem(region="us-west-2")
    keys = list(data_keys(args.bucket, args.prefix))

    def schema(key):
        s = pq.read_schema(f"{args.bucket}/{key}", filesystem=fs)
        return tuple((f.name, str(f.type)) for f in s.remove_metadata())

    with ThreadPoolExecutor(args.workers) as ex:
        schemas = dict(zip(keys, ex.map(schema, keys)))
    groups = collections.defaultdict(list)
    for key, s in schemas.items():
        groups[s].append(key)
    print(f"{len(keys)} files, {len(groups)} distinct schemas")
    for s, files in sorted(groups.items(), key=lambda g: -len(g[1])):
        print(f"  {len(files)} files with {len(s)} columns, e.g. {files[0]}")
    if len(groups) > 1:
        columns = [dict(s) for s in groups]
        names = set().union(*columns)
        for name in sorted(names):
            types = [c.get(name, "(missing)") for c in columns]
            if len(set(types)) > 1:
                print(f"  {name}: {' / '.join(types)}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", required=True)
    p.add_argument("--workers", type=int, default=32)
    main(p.parse_args())
