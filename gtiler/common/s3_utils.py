import boto3
import fsspec

def assume_role_credentials(ssm_parameter_name):
    # Create a session using your current credentials
    session = boto3.Session()

    # Retrieve the SSM parameter
    ssm = session.client("ssm", "us-west-2")
    parameter = ssm.get_parameter(Name=ssm_parameter_name, WithDecryption=True)
    parameter_value = parameter["Parameter"]["Value"]

    # Assume the DAAC access role
    sts = session.client("sts")
    assumed_role_object = sts.assume_role(
        RoleArn=parameter_value, RoleSessionName="TutorialSession"
    )

    # From the response that contains the assumed role, get the temporary
    # credentials that can be used to make subsequent API calls
    credentials = assumed_role_object["Credentials"]

    return credentials


def fsspec_access(credentials):
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