import os
import time
from typing import Optional

import boto3
from botocore.client import Config
from prometheus_client import Counter, Histogram

ENDPOINT = os.getenv("ORACLE_S3_ENDPOINT")
REGION = os.getenv("ORACLE_S3_REGION")
DEFAULT_BUCKET = os.getenv("ORACLE_S3_BUCKET")
BUCKET_TEMPLATE = os.getenv("ORACLE_S3_BUCKET_TEMPLATE")
AK = os.getenv("ORACLE_S3_ACCESS_KEY_ID")
SK = os.getenv("ORACLE_S3_SECRET_ACCESS_KEY")
PRESIGN_TTL = int(os.getenv("PRESIGN_URL_TTL_SECONDS", "900"))
DEFAULT_TENANT = os.getenv("DEFAULT_TENANT_ID", "demo")
PREFIX_TEMPLATE = os.getenv("ORACLE_S3_PREFIX_TEMPLATE", "{tenant}/")

_session = boto3.session.Session()
s3 = _session.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=AK,
    aws_secret_access_key=SK,
    region_name=REGION,
    config=Config(s3={"addressing_style": "virtual"}),
)

storage_latency = Histogram(
    "storage_io_latency_ms",
    "Latency in milliseconds for storage operations",
    ["operation", "tenant"],
)

storage_errors = Counter(
    "storage_errors_total",
    "Total number of storage errors",
    ["operation", "tenant"],
)


def _observe_latency(operation: str, tenant: str, start_time: float) -> None:
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    storage_latency.labels(operation=operation, tenant=tenant).observe(elapsed_ms)


def _increment_error(operation: str, tenant: str) -> None:
    storage_errors.labels(operation=operation, tenant=tenant).inc()


def _bucket_for_tenant(tenant: str) -> str:
    bucket = BUCKET_TEMPLATE.format(tenant=tenant) if BUCKET_TEMPLATE else DEFAULT_BUCKET
    if not bucket:
        raise RuntimeError("S3 bucket is not configured")
    return bucket


def _prefix_for_tenant(tenant: str) -> str:
    prefix = PREFIX_TEMPLATE.format(tenant=tenant) if PREFIX_TEMPLATE else ""
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"
    return prefix


def _full_key(tenant: str, key: str) -> str:
    prefix = _prefix_for_tenant(tenant)
    return f"{prefix}{key}" if prefix else key


def _with_metrics(operation: str, tenant: str, action):
    start = time.perf_counter()
    try:
        return action()
    except Exception:
        _increment_error(operation, tenant)
        raise
    finally:
        _observe_latency(operation, tenant, start)


def presign_put(
    key: str,
    content_type: str = "application/octet-stream",
    tenant_id: Optional[str] = None,
):
    tenant = tenant_id or DEFAULT_TENANT
    bucket = _bucket_for_tenant(tenant)
    object_key = _full_key(tenant, key)

    def _call():
        return s3.generate_presigned_post(
            bucket,
            object_key,
            Fields={"Content-Type": content_type},
            Conditions=[["starts-with", "$Content-Type", ""]],
            ExpiresIn=PRESIGN_TTL,
        )

    data = _with_metrics("presign_put", tenant, _call)
    return {
        "url": data["url"],
        "fields": data["fields"],
        "expires_in": PRESIGN_TTL,
        "key": object_key,
        "bucket": bucket,
    }


def presign_get(key: str, tenant_id: Optional[str] = None):
    tenant = tenant_id or DEFAULT_TENANT
    bucket = _bucket_for_tenant(tenant)
    object_key = _full_key(tenant, key)

    def _call():
        return s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket, "Key": object_key},
            ExpiresIn=PRESIGN_TTL,
        )

    url = _with_metrics("presign_get", tenant, _call)
    return {"url": url, "expires_in": PRESIGN_TTL, "key": object_key, "bucket": bucket}


def put(
    key: str,
    data: bytes,
    content_type: Optional[str] = None,
    tenant_id: Optional[str] = None,
):
    tenant = tenant_id or DEFAULT_TENANT
    bucket = _bucket_for_tenant(tenant)
    object_key = _full_key(tenant, key)

    def _call():
        kwargs = {"Bucket": bucket, "Key": object_key, "Body": data}
        if content_type:
            kwargs["ContentType"] = content_type
        return s3.put_object(**kwargs)

    return _with_metrics("put", tenant, _call)


def get(key: str, tenant_id: Optional[str] = None) -> bytes:
    tenant = tenant_id or DEFAULT_TENANT
    bucket = _bucket_for_tenant(tenant)
    object_key = _full_key(tenant, key)

    def _call():
        response = s3.get_object(Bucket=bucket, Key=object_key)
        return response["Body"].read()

    return _with_metrics("get", tenant, _call)
