import boto3
import fsspec


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
        print("Assuming role to access S3...")
        # Create a session using the default personal credentials
        session = boto3.Session()

        print("Retrieving SSM parameter for role ARN...")
        # Retrieve the SSM parameter
        ssm = session.client("ssm", "us-west-2")
        parameter = ssm.get_parameter(
            Name=ssm_parameter_name, WithDecryption=True
        )
        parameter_value = parameter["Parameter"]["Value"]
        print(f"Assuming role: {parameter_value}")

        # Assume the DAAC access role
        sts = session.client("sts")
        assumed_role_object = sts.assume_role(
            RoleArn=parameter_value,
            RoleSessionName="TutorialSession",
        )

        # From the response that contains the assumed role, get the temporary
        # credentials that can be used to make subsequent API calls
        credentials = assumed_role_object["Credentials"]
        print("Role assumed, temporary credentials obtained.")

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