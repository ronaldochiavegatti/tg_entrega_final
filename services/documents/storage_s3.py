import os
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import boto3
from botocore.client import Config
from prometheus_client import Counter, Histogram, REGISTRY

BACKEND = os.getenv("STORAGE_BACKEND", "s3").lower()
ENDPOINT = os.getenv("ORACLE_S3_ENDPOINT")
REGION = os.getenv("ORACLE_S3_REGION")
DEFAULT_BUCKET = os.getenv("ORACLE_S3_BUCKET")
BUCKET_TEMPLATE = os.getenv("ORACLE_S3_BUCKET_TEMPLATE")
AK = os.getenv("ORACLE_S3_ACCESS_KEY_ID")
SK = os.getenv("ORACLE_S3_SECRET_ACCESS_KEY")
PRESIGN_TTL = int(os.getenv("PRESIGN_URL_TTL_SECONDS", "900"))
DEFAULT_TENANT = os.getenv("DEFAULT_TENANT_ID", "demo")
PREFIX_TEMPLATE = os.getenv("ORACLE_S3_PREFIX_TEMPLATE", "{tenant}/")

FILESYSTEM_ROOT = Path(os.getenv("FILESYSTEM_STORAGE_ROOT", "/tmp/documents_storage"))
FILESYSTEM_BUCKET = os.getenv("FILESYSTEM_BUCKET_NAME", "localfs")

_session = boto3.session.Session() if BACKEND == "s3" else None
s3 = (
    _session.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=AK,
        aws_secret_access_key=SK,
        region_name=REGION,
        config=Config(s3={"addressing_style": "virtual"}),
    )
    if BACKEND == "s3"
    else None
)

try:
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
except ValueError:
    collectors = getattr(REGISTRY, "_names_to_collectors", {})
    storage_latency = collectors.get("storage_io_latency_ms")
    storage_errors = collectors.get("storage_errors_total")


def _observe_latency(operation: str, tenant: str, start_time: float) -> None:
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    storage_latency.labels(operation=operation, tenant=tenant).observe(elapsed_ms)


def _increment_error(operation: str, tenant: str) -> None:
    storage_errors.labels(operation=operation, tenant=tenant).inc()


def _bucket_for_tenant(tenant: str) -> str:
    if BACKEND == "filesystem":
        return FILESYSTEM_BUCKET
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


def _filesystem_path(tenant: str, key: str) -> Path:
    object_key = _full_key(tenant, key)
    path = FILESYSTEM_ROOT / object_key
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


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
        if BACKEND == "filesystem":
            path = _filesystem_path(tenant, key)
            return {
                "url": path.as_uri(),
                "fields": {},
                "expires_in": PRESIGN_TTL,
                "key": object_key,
                "bucket": bucket,
            }

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
        "fields": data.get("fields", {}),
        "expires_in": PRESIGN_TTL,
        "key": object_key,
        "bucket": bucket,
    }


def presign_get(key: str, tenant_id: Optional[str] = None):
    tenant = tenant_id or DEFAULT_TENANT
    bucket = _bucket_for_tenant(tenant)
    object_key = _full_key(tenant, key)

    def _call():
        if BACKEND == "filesystem":
            return _filesystem_path(tenant, key).as_uri()

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
        if BACKEND == "filesystem":
            path = _filesystem_path(tenant, key)
            path.write_bytes(data)
            return {"path": str(path), "key": object_key}
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
        if BACKEND == "filesystem":
            path = _filesystem_path(tenant, key)
            return path.read_bytes()
        response = s3.get_object(Bucket=bucket, Key=object_key)
        return response["Body"].read()

    return _with_metrics("get", tenant, _call)


def upload_via_presign(
    presigned: dict, data: bytes, content_type: Optional[str] = "application/octet-stream"
):
    tenant = presigned.get("tenant_id") or DEFAULT_TENANT

    def _call():
        if presigned.get("url", "").startswith("file://") or BACKEND == "filesystem":
            parsed = urlparse(presigned.get("url", ""))
            target = Path(parsed.path) if parsed.path else _filesystem_path(tenant, presigned["key"])
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            return {"path": str(target), "key": presigned["key"]}

        return s3.put_object(
            Bucket=presigned["bucket"],
            Key=presigned["key"],
            Body=data,
            ContentType=content_type,
        )

    return _with_metrics("upload_via_presign", tenant, _call)
