import boto3
from botocore.exceptions import ClientError
import fsspec
import logging
from maap.maap import MAAP
import requests
import time

logger = logging.getLogger(__name__)


# Each DAAC issues its own temporary S3 credentials, good only for its own
# buckets. They last an hour.
DAAC_CREDENTIALS_ENDPOINTS = {
    "lp-prod-protected": "https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials",
    "ornl-cumulus-prod-protected": "https://data.ornldaac.earthdata.nasa.gov/s3credentials",
}


# Waits between attempts at a MAAP API call. Under load the API times out,
# so calls retry connection failures and timeouts for about 8 minutes.
MAAP_API_WAITS = (15, 30, 60, 120, 240)


def call_maap_api(f, *args, **kwargs):
    """Call a MAAP API function, retrying connection failures and
    timeouts with backoff."""
    for wait in MAAP_API_WAITS:
        try:
            return f(*args, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as e:
            logger.warning("MAAP API call failed, retrying in %ds: %r", wait, e)
            time.sleep(wait)
    return f(*args, **kwargs)


class DaacFS:
    """S3 filesystems for the DAAC buckets, on temporary Earthdata credentials.

    Holds one filesystem per bucket, created on first use. Callers call
    refresh() when a read fails, e.g. once the credentials have expired.
    """

    def __init__(self):
        self.maap = call_maap_api(MAAP, maap_host="api.maap-project.org")
        self._fs = {}

    def get_fs(self, s3url):
        bucket = _bucket(s3url)
        if bucket not in self._fs:
            self.refresh(s3url)
        return self._fs[bucket]

    def refresh(self, s3url):
        bucket = _bucket(s3url)
        endpoint = DAAC_CREDENTIALS_ENDPOINTS[bucket]
        credentials = call_maap_api(self.maap.aws.earthdata_s3_credentials, endpoint)
        self._fs[bucket] = fsspec.filesystem(
            "s3",
            key=credentials["accessKeyId"],
            secret=credentials["secretAccessKey"],
            token=credentials["sessionToken"],
            skip_instance_cache=True,
            config_kwargs={
                "read_timeout": 120,
                "connect_timeout": 10,
                "retries": {"max_attempts": 3, "mode": "adaptive"},
            },
        )
        logger.info(
            "Obtained Earthdata S3 credentials for %s, expiring %s.",
            bucket,
            credentials["expiration"],
        )


def _bucket(s3url):
    return s3url.removeprefix("s3://").split("/", 1)[0]


def s3_prefix_exists(s3_path: str) -> bool:
    """Check if an S3 prefix exists.

    Args:
        s3_path: S3 path to check (e.g. s3://bucket/prefix/)
    Returns:
        True if the prefix exists, False otherwise.
    """
    fs = fsspec.filesystem("s3")
    return fs.exists(s3_path)


def object_etag(bucket: str, key: str) -> str:
    """The object's ETag, or None if it does not exist."""
    try:
        return boto3.client("s3").head_object(Bucket=bucket, Key=key)["ETag"]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return None
        raise


def conditional_put(
    bucket: str,
    key: str,
    body: bytes,
    *,
    if_match: str = None,
    if_none_match: str = None,
) -> str:
    """Write a small object on the same conditions as
    conditional_multipart_put. Returns the new ETag."""
    kw = {}
    if if_match is not None:
        kw["IfMatch"] = if_match
    if if_none_match is not None:
        kw["IfNoneMatch"] = if_none_match
    return boto3.client("s3").put_object(
        Bucket=bucket, Key=key, Body=body, **kw
    )["ETag"]


def conditional_multipart_put(
    bucket: str,
    key: str,
    body,
    *,
    if_match: str = None,
    if_none_match: str = None,
) -> str:
    """Upload a file-like object to S3 using multipart upload with a conditional write.

    The condition is evaluated atomically at CompleteMultipartUpload time.

    Args:
        body: A readable, binary file-like object positioned at the start of
            the payload. Read in 5 MB chunks so peak memory stays bounded
            regardless of payload size.
        if_match: Require the existing object to have this ETag (for updates).
        if_none_match: Pass "*" to require the object not to exist (for creates).
    Returns:
        The ETag of the newly written object.
    Raises:
        botocore.exceptions.ClientError with code "PreconditionFailed"
            if the condition is not satisfied.
    """
    s3_client = boto3.client("s3")
    mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = mpu["UploadId"]

    try:
        parts = []
        chunk_size = 5 * 1024 * 1024  # 5 MB minimum for non-final parts
        part_number = 1
        while True:
            chunk = body.read(chunk_size)
            if not chunk:
                break
            resp = s3_client.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=chunk,
            )
            parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})
            part_number += 1

        complete_kwargs = {
            "Bucket": bucket,
            "Key": key,
            "UploadId": upload_id,
            "MultipartUpload": {"Parts": parts},
        }
        if if_match is not None:
            complete_kwargs["IfMatch"] = if_match
        if if_none_match is not None:
            complete_kwargs["IfNoneMatch"] = if_none_match

        response = s3_client.complete_multipart_upload(**complete_kwargs)
        return response["ETag"]

    except Exception:
        s3_client.abort_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id
        )
        raise
