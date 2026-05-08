import boto3
import fsspec
import logging

logger = logging.getLogger(__name__)


class RefreshableFSSpec:
    def __init__(self, ssm_parameter_name):
        self.ssm_parameter_name = ssm_parameter_name
        self.credentials = self.assume_role_credentials(self.ssm_parameter_name)
        self.fs = self.fsspec_access(self.credentials)

    def refresh(self):
        self.credentials = self.assume_role_credentials(self.ssm_parameter_name)
        self.fs = self.fsspec_access(self.credentials)

    def get_fs(self):
        return self.fs

    def assume_role_credentials(self, ssm_parameter_name):
        logger.info("Assuming role to access S3...")
        # Create a session using the default personal credentials
        session = boto3.Session()

        logger.info("Retrieving SSM parameter for role ARN...")
        # Retrieve the SSM parameter
        ssm = session.client("ssm", "us-west-2")
        parameter = ssm.get_parameter(
            Name=ssm_parameter_name, WithDecryption=True
        )
        parameter_value = parameter["Parameter"]["Value"]
        logger.info("Assuming role: %s", parameter_value)

        # Assume the DAAC access role
        sts = session.client("sts")
        assumed_role_object = sts.assume_role(
            RoleArn=parameter_value,
            RoleSessionName="TutorialSession",
        )

        # From the response that contains the assumed role, get the temporary
        # credentials that can be used to make subsequent API calls
        credentials = assumed_role_object["Credentials"]
        logger.info("Role assumed, temporary credentials obtained.")

        return credentials

    def fsspec_access(self, credentials):
        fsspec_kwargs = {
            "default_cache_type": "mmap",
            "default_block_size": 5 * 1024 * 1024,  # fsspec default is 5 MB
            "default_fill_cache": True,
        }
        return fsspec.filesystem(
            "s3",
            key=credentials["AccessKeyId"],
            secret=credentials["SecretAccessKey"],
            token=credentials["SessionToken"],
            requester_pays=True,
            **fsspec_kwargs,
        )


def s3_prefix_exists(s3_path: str) -> bool:
    """Check if an S3 prefix exists.

    Args:
        s3_path: S3 path to check (e.g. s3://bucket/prefix/)
    Returns:
        True if the prefix exists, False otherwise.
    """
    fs = fsspec.filesystem("s3")
    return fs.exists(s3_path)


def conditional_multipart_put(
    bucket: str, key: str, data: bytes, *, if_match: str = None, if_none_match: str = None
) -> str:
    """Upload bytes to S3 using multipart upload with a conditional write.

    The condition is evaluated atomically at CompleteMultipartUpload time.

    Args:
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
        for offset in range(0, len(data), chunk_size):
            chunk = data[offset : offset + chunk_size]
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
        s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise