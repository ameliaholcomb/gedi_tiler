"""Probe which credentials can read GEDI granules from DPS.

Tries every combination of credentials, file, requester-pays setting and
read method, and prints one line per attempt. Nothing here raises: each
failure is reported with its error code so the whole matrix always runs.
"""

import logging
import sys
import traceback

import boto3
import fsspec
import h5py
import s3fs
from maap.maap import MAAP

from gtiler.common import s3_utils

logger = logging.getLogger(__name__)

# Granule O08275_04 (tile S23_W047), in both collection versions, plus a
# file in our own workspace bucket as a control.
URLS = {
    "lp_l2a_v002": "s3://lp-prod-protected/GEDI02_A.002/GEDI02_A_2020150040608_O08275_04_T00954_02_003_01_V002/GEDI02_A_2020150040608_O08275_04_T00954_02_003_01_V002.h5",
    "lp_l2a_v003": "s3://lp-prod-protected/GEDI02_A.003/GEDI02_A_2020150040608_O08275_04_T00954_02_004_02_V003/GEDI02_A_2020150040608_O08275_04_T00954_02_004_02_V003.h5",
    "ornl_l4a_v002": "s3://ornl-cumulus-prod-protected/gedi/GEDI_L4A_AGB_Density_V2_1/data/GEDI04_A_2020150040608_O08275_04_T00954_02_002_02_V002.h5",
    "ornl_l4a_v003": "s3://ornl-cumulus-prod-protected/gedi/GEDI_L4A_AGB_Density_V3/data/GEDI04_A_2020150040608_O08275_04_T00954_02_004_01_V003.h5",
    "own_bucket": "s3://maap-ops-workspace/shared/ameliah/gedi-test/test_v16/metadata/tile_id=S23_W047/data_0.parquet",
}

EARTHDATA_ENDPOINTS = {
    "earthdata_lp": "https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials",
    "earthdata_ornl": "https://data.ornldaac.earthdata.nasa.gov/s3credentials",
}


def default_credentials():
    """The worker's own role, as boto finds it."""
    c = boto3.Session().get_credentials().get_frozen_credentials()
    return {"key": c.access_key, "secret": c.secret_key, "token": c.token}


def maap_data_reader_credentials():
    session = boto3.Session()
    arn = session.client("ssm", "us-west-2").get_parameter(
        Name="/iam/maap-data-reader", WithDecryption=True
    )["Parameter"]["Value"]
    c = session.client("sts").assume_role(
        RoleArn=arn, RoleSessionName="gedi-s3-probe"
    )["Credentials"]
    return {
        "key": c["AccessKeyId"],
        "secret": c["SecretAccessKey"],
        "token": c["SessionToken"],
    }


def earthdata_credentials(maap, endpoint):
    c = maap.aws.earthdata_s3_credentials(endpoint)
    return {
        "key": c["accessKeyId"],
        "secret": c["secretAccessKey"],
        "token": c["sessionToken"],
    }


def gather_credentials():
    creds = {}
    sources = {
        "default": default_credentials,
        "maap_data_reader": maap_data_reader_credentials,
    }
    maap = MAAP(maap_host="api.maap-project.org")
    for name, endpoint in EARTHDATA_ENDPOINTS.items():
        sources[name] = lambda e=endpoint: earthdata_credentials(maap, e)
    for name, fn in sources.items():
        try:
            creds[name] = fn()
        except Exception as e:
            print(f"CREDS {name}: FAILED {type(e).__name__}: {e}")
            continue
        try:
            arn = boto3.client(
                "sts",
                aws_access_key_id=creds[name]["key"],
                aws_secret_access_key=creds[name]["secret"],
                aws_session_token=creds[name]["token"],
            ).get_caller_identity()["Arn"]
        except Exception as e:
            arn = f"unknown ({type(e).__name__}: {e})"
        print(f"CREDS {name}: {arn}")
    return creds


def _split(url):
    bucket, key = url[len("s3://") :].split("/", 1)
    return bucket, key


def boto_head(c, url, requester_pays):
    bucket, key = _split(url)
    kw = {"RequestPayer": "requester"} if requester_pays else {}
    client = boto3.client(
        "s3",
        region_name="us-west-2",
        aws_access_key_id=c["key"],
        aws_secret_access_key=c["secret"],
        aws_session_token=c["token"],
    )
    size = client.head_object(Bucket=bucket, Key=key, **kw)["ContentLength"]
    return f"{size} bytes"


def boto_range_get(c, url, requester_pays):
    bucket, key = _split(url)
    kw = {"RequestPayer": "requester"} if requester_pays else {}
    client = boto3.client(
        "s3",
        region_name="us-west-2",
        aws_access_key_id=c["key"],
        aws_secret_access_key=c["secret"],
        aws_session_token=c["token"],
    )
    body = client.get_object(Bucket=bucket, Key=key, Range="bytes=0-1023", **kw)
    return f"read {len(body['Body'].read())} bytes"


def s3fs_read(c, url, requester_pays):
    # Same construction as gtiler.common.s3_utils.RefreshableFSSpec.
    fs = s3fs.S3FileSystem(
        key=c["key"],
        secret=c["secret"],
        token=c["token"],
        requester_pays=requester_pays,
        skip_instance_cache=True,
    )
    with fs.open(url, "rb") as f:
        return f"read {len(f.read(1024))} bytes"


def _h5_summary(f):
    with h5py.File(f) as hdf5:
        beam = sorted(k for k in hdf5.keys() if k.startswith("BEAM"))[0]
        shots = hdf5[f"{beam}/shot_number"][:10]
        return f"streamed {len(shots)} shot numbers from {beam}"


def h5py_stream(c, url, requester_pays):
    # The pre-download builder read: h5py over an fsspec file, with the
    # filesystem built exactly as in s3_utils.RefreshableFSSpec.
    fs = fsspec.filesystem(
        "s3",
        key=c["key"],
        secret=c["secret"],
        token=c["token"],
        requester_pays=requester_pays,
        config_kwargs={
            "read_timeout": 120,
            "connect_timeout": 10,
            "retries": {"max_attempts": 3, "mode": "adaptive"},
        },
        default_cache_type="mmap",
        default_block_size=5 * 1024 * 1024,
        default_fill_cache=True,
        skip_instance_cache=True,
    )
    with fs.open(url, mode="rb") as f:
        return _h5_summary(f)


METHODS = {
    "boto_head": boto_head,
    "boto_range_get": boto_range_get,
    "s3fs_read": s3fs_read,
    "h5py_stream": h5py_stream,
}


def old_builder_reads():
    """The exact v2 builder path: RefreshableFSSpec, then h5py over open()."""
    print()
    try:
        rfs = s3_utils.RefreshableFSSpec("/iam/maap-data-reader")
    except Exception as e:
        print(f"OLD_BUILDER RefreshableFSSpec: FAIL {_error_code(e)}")
        return
    for uname, url in URLS.items():
        if not url.endswith(".h5"):
            continue
        try:
            with rfs.get_fs().open(url, mode="rb") as f:
                result = "OK " + _h5_summary(f)
        except Exception as e:
            result = "FAIL " + _error_code(e)
        print(f"OLD_BUILDER {uname:<15} {result}")


def _error_code(e):
    code = getattr(e, "response", {}).get("Error", {}).get("Code")
    if code:
        return code
    return f"{type(e).__name__}: {e}".splitlines()[0][:120]


def main():
    creds = gather_credentials()
    print()
    print(f"{'creds':<18} {'file':<15} {'rp':<5} {'method':<15} result")
    ok = 0
    total = 0
    for cname, c in creds.items():
        for uname, url in URLS.items():
            for rp in (True, False):
                for mname, method in METHODS.items():
                    if mname == "h5py_stream" and not url.endswith(".h5"):
                        continue
                    total += 1
                    try:
                        result = "OK " + method(c, url, rp)
                        ok += 1
                    except Exception as e:
                        result = "FAIL " + _error_code(e)
                        logger.debug(traceback.format_exc())
                    print(f"{cname:<18} {uname:<15} {str(rp):<5} {mname:<15} {result}")
    print()
    print(f"{ok} of {total} reads succeeded.")
    old_builder_reads()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    main()
