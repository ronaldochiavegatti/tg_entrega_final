import datetime as dt
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from celery import Celery
from fastapi import Body, Depends, FastAPI, Header, HTTPException, Request, Response
from opentelemetry import baggage, context, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, REGISTRY, generate_latest
from pydantic import BaseModel, Field
from pymongo import MongoClient
import psycopg

from common.auth import require_roles
from common.logging import configure_structured_logging
from .storage_s3 import DEFAULT_TENANT, presign_get, presign_put


configure_structured_logging("documents")
logger = logging.getLogger("documents")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "app")
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN", "postgresql://postgres:postgres@postgres:5432/app"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

EVENT_DOCUMENT_PROCESSED = "DOCUMENT_PROCESSED"
EVENT_FIELDS_UPDATED = "FIELDS_UPDATED"
EVENT_LIMITS_RECALCULATED = "LIMITS_RECALCULATED"

celery_app = Celery("documents", broker=REDIS_URL, backend=REDIS_URL)

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB]
documents_collection = mongo_db["documents"]

pg_conn = psycopg.connect(POSTGRES_DSN, autocommit=True)

try:
    REQUEST_LATENCY = Histogram(
        "http_request_latency_seconds",
        "HTTP request latency in seconds",
        ["service", "method", "path"],
    )
    ERROR_COUNT = Counter(
        "error_count",
        "Total number of HTTP errors",
        ["service", "method", "path", "status_code"],
    )
except ValueError:
    collectors = getattr(REGISTRY, "_names_to_collectors", {})
    REQUEST_LATENCY = collectors.get("http_request_latency_seconds") or Histogram(
        "http_request_latency_seconds",
        "HTTP request latency in seconds",
        ["service", "method", "path"],
        registry=None,
    )
    ERROR_COUNT = collectors.get("error_count") or Counter(
        "error_count",
        "Total number of HTTP errors",
        ["service", "method", "path", "status_code"],
        registry=None,
    )


def _setup_tracer(service_name: str) -> None:
    resource = Resource.create({"service.name": service_name})
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(tracer_provider)
    RequestsInstrumentor().instrument()


def _configure_observability(app: FastAPI, service_name: str) -> None:
    _setup_tracer(service_name)
    FastAPIInstrumentor.instrument_app(app)

    @app.middleware("http")
    async def _metrics_and_baggage(request: Request, call_next):
        start = time.perf_counter()
        ctx = context.get_current()
        tenant_id = request.headers.get("x-tenant-id")
        user_id = request.headers.get("x-user-id")
        if tenant_id:
            ctx = baggage.set_baggage("tenant_id", tenant_id, context=ctx)
        if user_id:
            ctx = baggage.set_baggage("user_id", user_id, context=ctx)
        token = context.attach(ctx)
        try:
            response = await call_next(request)
        except Exception:
            ERROR_COUNT.labels(service_name, request.method, request.url.path, "500").inc()
            raise
        finally:
            elapsed = time.perf_counter() - start
            REQUEST_LATENCY.labels(service_name, request.method, request.url.path).observe(elapsed)
            context.detach(token)
        if response.status_code >= 400:
            ERROR_COUNT.labels(
                service_name, request.method, request.url.path, str(response.status_code)
            ).inc()
        return response

    @app.get("/metrics")
    def _metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


app = FastAPI(title="documents")
_configure_observability(app, "documents")


def _resolve_tenant(tenant_id: Optional[str]) -> str:
    return tenant_id or DEFAULT_TENANT


def _ensure_audit_table() -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_field_changes (
                id SERIAL PRIMARY KEY,
                doc_id TEXT NOT NULL,
                user_id TEXT,
                ts TIMESTAMPTZ DEFAULT NOW(),
                field TEXT NOT NULL,
                old_value JSONB,
                new_value JSONB,
                source TEXT NOT NULL
            );
            """
        )


@app.on_event("startup")
def on_startup() -> None:
    _ensure_audit_table()


@app.on_event("shutdown")
def on_shutdown() -> None:
    mongo_client.close()
    pg_conn.close()


class PresignUploadReq(BaseModel):
    key: str
    content_type: Optional[str] = "application/pdf"
    tenant_id: Optional[str] = None


class DocumentField(BaseModel):
    name: str
    value: Any = None
    confidence: Optional[float] = None
    source: str = "extracted"


class Totals(BaseModel):
    gross_amount: float = 0.0


class Document(BaseModel):
    id: str = Field(alias="_id")
    tenant_id: str
    date: Optional[str] = None
    type: Optional[str] = None
    totals: Totals = Field(default_factory=Totals)
    fields: List[DocumentField] = Field(default_factory=list)
    storage: Dict[str, Any] = Field(default_factory=dict)
    ocr_meta: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        populate_by_name = True


class PatchChange(BaseModel):
    path: str
    value: object
    source: str = "user_corrected"


class BulkPatchItem(BaseModel):
    doc_id: str
    changes: List[PatchChange]
    user_id: Optional[str] = None


class BulkPatchRequest(BaseModel):
    items: List[BulkPatchItem]


def _publish_event(event_name: str, payload: Dict[str, Any]) -> None:
    try:
        celery_app.send_task(event_name, kwargs={"payload": payload})
    except Exception as exc:  # pragma: no cover - broker optional in dev
        logger.warning("Failed to publish event %s: %s", event_name, exc)


def _serialize_document(data: Dict[str, Any]) -> Dict[str, Any]:
    if not data:
        return data
    serialized = data.copy()
    doc_id = serialized.get("_id")
    if not isinstance(doc_id, str):
        serialized["_id"] = str(doc_id)
    return serialized


def _validate_cnpj(value: str) -> bool:
    return bool(re.fullmatch(r"\d{14}", value))


def _validate_date(value: str) -> bool:
    try:
        dt.date.fromisoformat(value)
        return True
    except ValueError:
        return False


def _validate_document(doc: Dict[str, Any]) -> None:
    totals = doc.get("totals", {})
    gross_amount = totals.get("gross_amount")
    if gross_amount is not None and float(gross_amount) < 0:
        raise HTTPException(status_code=400, detail="gross_amount must be non-negative")

    for field in doc.get("fields", []):
        name = field.get("name")
        value = field.get("value")
        if name == "cnpj" and value is not None and not _validate_cnpj(str(value)):
            raise HTTPException(status_code=400, detail="Invalid CNPJ format")
        if name == "date" and value is not None and not _validate_date(str(value)):
            raise HTTPException(status_code=400, detail="Invalid date format")


def _record_audit(
    doc_id: str,
    field: str,
    old: Any,
    new: Any,
    source: str,
    user_id: Optional[str],
) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit_field_changes (doc_id, user_id, field, old_value, new_value, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [doc_id, user_id, field, json.dumps(old), json.dumps(new), source],
        )


def _upsert_field(fields: List[Dict[str, Any]], name: str, value: Any, source: str) -> Tuple[Any, Any]:
    for field in fields:
        if field.get("name") == name:
            old = field.get("value")
            field.update({"value": value, "source": source})
            return old, value
    fields.append({"name": name, "value": value, "source": source})
    return None, value


def _apply_change(doc: Dict[str, Any], change: PatchChange) -> Tuple[str, Any, Any]:
    path = change.path
    value = change.value
    if path == "totals.gross_amount":
        old = doc.setdefault("totals", {}).get("gross_amount")
        doc["totals"]["gross_amount"] = float(value)
        return path, old, doc["totals"]["gross_amount"]
    if path.startswith("fields."):
        field_name = path.split(".", 1)[1]
        fields = doc.setdefault("fields", [])
        old, new = _upsert_field(fields, field_name, value, change.source)
        return field_name, old, new
    if path.startswith("storage."):
        key = path.split(".", 1)[1]
        old = doc.setdefault("storage", {}).get(key)
        doc["storage"][key] = value
        return path, old, value
    if path == "date":
        old = doc.get("date")
        doc["date"] = str(value)
        return path, old, doc["date"]
    if path == "type":
        old = doc.get("type")
        doc["type"] = str(value)
        return path, old, doc["type"]
    raise HTTPException(status_code=400, detail=f"Unsupported path: {path}")


def _save_document(doc: Dict[str, Any]) -> None:
    documents_collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)


def _get_document_or_404(doc_id: str) -> Dict[str, Any]:
    doc = documents_collection.find_one({"_id": doc_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    return doc


def _run_ocr_pipeline(doc_id: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
    doc = _get_document_or_404(doc_id)
    text = str(doc.get("storage", {}).get("mock_text", ""))
    meta = doc.setdefault("ocr_meta", {})
    meta["tenant_id"] = tenant_id or doc.get("tenant_id", DEFAULT_TENANT)
    meta["pre_processing"] = {"deskew": True, "binarization": True}
    meta["engine"] = {"language": ["por", "eng"], "provider": "tesseract"}

    amount_match = re.search(r"(total|valor)\D+(\d+[\.,]\d+)", text, re.IGNORECASE)
    if amount_match:
        value_raw = amount_match.group(2).replace(",", ".")
        _upsert_field(doc.setdefault("fields", []), "total", float(value_raw), "extracted")
        doc.setdefault("totals", {})["gross_amount"] = float(value_raw)

    cnpj_match = re.search(r"(\d{14})", text)
    if cnpj_match:
        _upsert_field(doc.setdefault("fields", []), "cnpj", cnpj_match.group(1), "extracted")

    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if date_match:
        doc["date"] = date_match.group(1)

    meta["validations"] = {"totals_present": "gross_amount" in doc.get("totals", {})}
    meta["processed_at"] = dt.datetime.utcnow().isoformat()

    _validate_document(doc)
    _save_document(doc)
    _publish_event(EVENT_DOCUMENT_PROCESSED, {"doc_id": doc_id, "tenant_id": meta["tenant_id"]})
    return _serialize_document(doc)


@celery_app.task(name="documents.process_document")
def process_document_task(doc_id: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
    return _run_ocr_pipeline(doc_id, tenant_id)


@app.post(
    "/storage/presign-upload", dependencies=[Depends(require_roles(["editor", "admin"]))]
)
def presign_upload(req: PresignUploadReq):
    tenant = _resolve_tenant(req.tenant_id)
    data = presign_put(req.key, req.content_type, tenant)
    return {"url": data["url"], "expires_in": data["expires_in"], "key": data["key"]}


@app.get("/storage/presign-download")
def presign_download(key: str, tenant_id: Optional[str] = None):
    tenant = _resolve_tenant(tenant_id)
    data = presign_get(key, tenant)
    return {"url": data["url"], "expires_in": data["expires_in"], "key": data["key"]}


@app.get("/documents/{doc_id}")
def get_doc(doc_id: str):
    doc = _get_document_or_404(doc_id)
    return _serialize_document(doc)


@app.patch("/documents/{doc_id}", dependencies=[Depends(require_roles(["editor", "admin"]))])
def patch_doc(
    doc_id: str,
    changes: List[PatchChange] = Body(...),
    x_user_id: Optional[str] = Header(default=None, alias="X-User-Id"),
):
    doc = _get_document_or_404(doc_id)
    updated_fields: List[str] = []
    for change in changes:
        field, old, new = _apply_change(doc, change)
        _record_audit(doc_id, field, old, new, change.source, x_user_id)
        updated_fields.append(field)

    _validate_document(doc)
    _save_document(doc)
    _publish_event(EVENT_FIELDS_UPDATED, {"doc_id": doc_id, "fields": updated_fields})
    if any(path.startswith("totals") for path in updated_fields):
        _publish_event(EVENT_LIMITS_RECALCULATED, {"doc_id": doc_id})
    return {"doc_id": doc_id, "updated": len(changes)}


@app.post("/documents/bulk", dependencies=[Depends(require_roles(["editor", "admin"]))])
def bulk_patch(req: BulkPatchRequest):
    results = []
    ok = 0
    errors = 0
    for item in req.items:
        try:
            patch_doc(item.doc_id, item.changes, item.user_id)
            ok += 1
            results.append({"doc_id": item.doc_id, "status": "ok"})
        except HTTPException as exc:
            errors += 1
            results.append({"doc_id": item.doc_id, "status": "error", "detail": exc.detail})
    return {"results": results, "summary": {"ok": ok, "errors": errors}}


@app.get("/documents/{doc_id}/audit")
def audit(doc_id: str):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT user_id, ts, field, old_value, new_value, source
            FROM audit_field_changes
            WHERE doc_id = %s
            ORDER BY ts ASC
            """,
            [doc_id],
        )
        rows = cur.fetchall()
    audit_trail = []
    for row in rows:
        user_id, ts, field, old_value, new_value, source = row
        audit_trail.append(
            {
                "user": user_id or "system",
                "ts": ts.isoformat(),
                "field": field,
                "old": old_value,
                "new": new_value,
                "source": source,
            }
        )
    return {"doc_id": doc_id, "audit": audit_trail}


@app.get("/documents/health")
def health():
    return {"ok": True}
