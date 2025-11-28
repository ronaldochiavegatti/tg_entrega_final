import os, boto3
from botocore.client import Config

ENDPOINT = os.getenv("ORACLE_S3_ENDPOINT")
REGION = os.getenv("ORACLE_S3_REGION")
BUCKET = os.getenv("ORACLE_S3_BUCKET")
AK = os.getenv("ORACLE_S3_ACCESS_KEY_ID")
SK = os.getenv("ORACLE_S3_SECRET_ACCESS_KEY")
TTL = int(os.getenv("PRESIGN_URL_TTL_SECONDS", "900"))

_session = boto3.session.Session()
s3 = _session.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=AK,
    aws_secret_access_key=SK,
    region_name=REGION,
    config=Config(s3={'addressing_style': 'virtual'})
)


def presign_put(key: str, content_type: str = "application/octet-stream"):
    return s3.generate_presigned_post(
        BUCKET,
        key,
        Fields={"Content-Type": content_type},
        Conditions=[["starts-with", "$Content-Type", ""]],
        ExpiresIn=TTL,
    )


def presign_get(key: str):
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": BUCKET, "Key": key},
        ExpiresIn=TTL,
    )
