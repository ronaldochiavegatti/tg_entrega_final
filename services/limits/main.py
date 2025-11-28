import datetime as dt
import logging
import os
import time
from collections import defaultdict
from typing import Dict, Iterable, List, Optional

from celery import Celery
from fastapi import Body, Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import StreamingResponse
from opentelemetry import baggage, context, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field
from pymongo import MongoClient

from .db import get_conn
from common.auth import require_roles
from common.logging import configure_structured_logging

configure_structured_logging("limits")
logger = logging.getLogger("limits")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "app")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

celery_app = Celery("limits", broker=REDIS_URL, backend=REDIS_URL)
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB]
documents_collection = mongo_db["documents"]

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
LIMITS_RECALC = Histogram(
    "limits_recalc_latency_ms",
    "Latency for recalculating limits in milliseconds",
    ["tenant", "year"],
)
LIMITS_STATE = Counter(
    "limits_state_count",
    "Count of calculated limit states",
    ["tenant", "year", "state"],
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


app = FastAPI(title="limits")
_configure_observability(app, "limits")

EVENT_FIELDS_UPDATED = "FIELDS_UPDATED"
EVENT_LIMITS_RECALCULATED = "LIMITS_RECALCULATED"

DEFAULT_ANNUAL_LIMIT = 81000.0
DEFAULT_WARN_THRESHOLD = 0.8
DEFAULT_CRITICAL_THRESHOLD = 1.0


class RecalcReq(BaseModel):
    tenant_id: str
    year: int
    doc_ids: Optional[list[str]] = None


class SimulationAdjustment(BaseModel):
    month: int = Field(ge=1, le=12)
    delta: float


class SimulationRequest(BaseModel):
    tenant_id: str
    year: int
    adjustments: List[SimulationAdjustment] = Field(default_factory=list)
    doc_ids: Optional[list[str]] = None


class DashboardResponse(BaseModel):
    accumulated: float
    forecast: float
    state: str
    threshold: Dict[str, float]
    months: List[Dict[str, float]]


STATE_OK = "OK"
STATE_NEAR_LIMIT = "NEAR_LIMIT"
STATE_AT_LIMIT = "AT_LIMIT"
STATE_EXCEEDED = "EXCEEDED"


@app.on_event("startup")
def on_startup() -> None:
    _ensure_tables()


@app.on_event("shutdown")
def on_shutdown() -> None:
    mongo_client.close()


def _ensure_tables() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS limit_config (
                    year INT PRIMARY KEY,
                    annual_limit NUMERIC NOT NULL,
                    warn_threshold NUMERIC NOT NULL DEFAULT 0.8,
                    critical_threshold NUMERIC NOT NULL DEFAULT 1.0
                );

                CREATE TABLE IF NOT EXISTS limits_snapshots (
                    tenant_id TEXT NOT NULL,
                    year INT NOT NULL,
                    month INT NOT NULL,
                    accumulated NUMERIC NOT NULL,
                    forecast NUMERIC NOT NULL,
                    state TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (tenant_id, year, month)
                );

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


def _publish_event(event_name: str, payload: Dict[str, object]) -> None:
    try:
        celery_app.send_task(event_name, kwargs={"payload": payload})
    except Exception as exc:  # pragma: no cover - broker optional
        logger.warning("Failed to publish event %s: %s", event_name, exc)


def _get_limit_config(year: int) -> Dict[str, float]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT annual_limit, warn_threshold, critical_threshold FROM limit_config WHERE year = %s",
                [year],
            )
            row = cur.fetchone()
            if row:
                annual_limit, warn, critical = row
                return {
                    "annual_limit": float(annual_limit),
                    "warn": float(warn),
                    "critical": float(critical),
                }
            cur.execute(
                """
                INSERT INTO limit_config(year, annual_limit, warn_threshold, critical_threshold)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (year) DO NOTHING
                """,
                [year, DEFAULT_ANNUAL_LIMIT, DEFAULT_WARN_THRESHOLD, DEFAULT_CRITICAL_THRESHOLD],
            )
            return {
                "annual_limit": DEFAULT_ANNUAL_LIMIT,
                "warn": DEFAULT_WARN_THRESHOLD,
                "critical": DEFAULT_CRITICAL_THRESHOLD,
            }


def _compute_state(ratio: float, warn: float, critical: float) -> str:
    if ratio < warn:
        return STATE_OK
    if ratio < critical:
        return STATE_NEAR_LIMIT
    if abs(ratio - 1.0) <= 0.01:
        return STATE_AT_LIMIT
    if ratio >= 1.0:
        return STATE_EXCEEDED
    return STATE_NEAR_LIMIT


def _extract_month(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        return int(str(value).split("-")[1])
    except (ValueError, IndexError):
        return None


def _collect_documents(tenant_id: str, year: int, doc_ids: Optional[Iterable[str]]) -> List[Dict[str, object]]:
    query: Dict[str, object] = {"tenant_id": tenant_id}
    if doc_ids:
        query["_id"] = {"$in": [str(doc_id) for doc_id in doc_ids]}
    date_prefix = f"{year}-"
    query["date"] = {"$regex": f"^{date_prefix}"}
    return list(documents_collection.find(query))


def _summaries_from_documents(docs: List[Dict[str, object]]) -> Dict[str, object]:
    monthly_totals: Dict[int, float] = defaultdict(float)
    for doc in docs:
        totals = doc.get("totals", {}) if isinstance(doc, dict) else {}
        amount = float(totals.get("gross_amount", 0.0) or 0.0)
        month = _extract_month(doc.get("date")) if isinstance(doc, dict) else None
        if month:
            monthly_totals[month] += amount
    accumulated = sum(monthly_totals.values())
    months_with_data = len([v for v in monthly_totals.values() if v > 0])
    months_with_data = months_with_data or dt.date.today().month
    forecast = (accumulated / months_with_data) * 12 if months_with_data else 0.0
    return {
        "monthly_totals": monthly_totals,
        "accumulated": accumulated,
        "forecast": forecast,
    }


def _persist_snapshots(
    tenant_id: str,
    year: int,
    monthly_totals: Dict[int, float],
    forecast: float,
    config: Dict[str, float],
) -> None:
    cumulative = 0.0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for month in range(1, 13):
                cumulative += monthly_totals.get(month, 0.0)
                ratio = max(cumulative, forecast) / config["annual_limit"] if config["annual_limit"] else 0.0
                state = _compute_state(ratio, config["warn"], config["critical"])
                cur.execute(
                    """
                    INSERT INTO limits_snapshots(tenant_id, year, month, accumulated, forecast, state, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (tenant_id, year, month) DO UPDATE
                    SET accumulated = EXCLUDED.accumulated,
                        forecast = EXCLUDED.forecast,
                        state = EXCLUDED.state,
                        updated_at = NOW()
                    """,
                    [tenant_id, year, month, cumulative, forecast, state],
                )


def _build_dashboard_payload(
    tenant_id: str, year: int, monthly_totals: Dict[int, float], accumulated: float, forecast: float
) -> DashboardResponse:
    config = _get_limit_config(year)
    ratio = max(accumulated, forecast) / config["annual_limit"] if config["annual_limit"] else 0.0
    state = _compute_state(ratio, config["warn"], config["critical"])
    months_payload = [{"month": m, "value": monthly_totals.get(m, 0.0)} for m in range(1, 13)]
    return DashboardResponse(
        accumulated=accumulated,
        forecast=forecast,
        state=state,
        threshold={"warn": config["warn"], "critical": config["critical"]},
        months=months_payload,
    )


def recalc_limits(tenant_id: str, year: int, doc_ids: Optional[list[str]] = None) -> DashboardResponse:
    start = time.perf_counter()
    docs = _collect_documents(tenant_id, year, doc_ids)
    summaries = _summaries_from_documents(docs)
    config = _get_limit_config(year)
    _persist_snapshots(tenant_id, year, summaries["monthly_totals"], summaries["forecast"], config)
    dashboard = _build_dashboard_payload(
        tenant_id,
        year,
        summaries["monthly_totals"],
        summaries["accumulated"],
        summaries["forecast"],
    )
    _publish_event(
        EVENT_LIMITS_RECALCULATED,
        {"tenant_id": tenant_id, "year": year, "state": dashboard.state, "accumulated": dashboard.accumulated},
    )
    elapsed_ms = (time.perf_counter() - start) * 1000
    LIMITS_RECALC.labels(str(tenant_id), str(year)).observe(elapsed_ms)
    LIMITS_STATE.labels(str(tenant_id), str(year), dashboard.state).inc()
    return dashboard


@celery_app.task(name=EVENT_FIELDS_UPDATED)
def fields_updated(payload: Dict[str, object]) -> Dict[str, object]:
    doc_id = payload.get("doc_id") if isinstance(payload, dict) else None
    if not doc_id:
        return {"status": "ignored", "reason": "missing doc_id"}
    doc = documents_collection.find_one({"_id": str(doc_id)})
    if not doc:
        return {"status": "ignored", "reason": "document not found"}
    year = int(str(doc.get("date", dt.date.today().isoformat())).split("-", 1)[0])
    tenant_id = doc.get("tenant_id") or "default"
    dashboard = recalc_limits(str(tenant_id), year, [str(doc_id)])
    return {"status": "recalculated", "state": dashboard.state}


@app.get("/limits/{year}/dashboard", response_model=DashboardResponse)
def dashboard(year: int, tenant_id: str):
    docs = _collect_documents(tenant_id, year, None)
    summaries = _summaries_from_documents(docs)
    return _build_dashboard_payload(
        tenant_id,
        year,
        summaries["monthly_totals"],
        summaries["accumulated"],
        summaries["forecast"],
    )


@app.get(
    "/limits/{year}/export", dependencies=[Depends(require_roles(["editor", "admin"]))]
)
def export(year: int, tenant_id: str, format: str = "csv"):
    dashboard_data = dashboard(year, tenant_id)
    if format != "csv":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only csv supported")

    def _csv_stream():
        yield "month,value\n"
        for month in dashboard_data.months:
            yield f"{month['month']},{month['value']}\n"

    return StreamingResponse(_csv_stream(), media_type="text/csv")


@app.post(
    "/limits/recalculate",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_roles(["editor", "admin"]))],
)
def recalc(body: RecalcReq = Body(...)):
    dashboard = recalc_limits(body.tenant_id, body.year, body.doc_ids)
    return {"accepted": True, "state": dashboard.state}


@app.post("/limits/simulate", response_model=DashboardResponse)
def simulate(req: SimulationRequest):
    docs = _collect_documents(req.tenant_id, req.year, req.doc_ids)
    summaries = _summaries_from_documents(docs)
    monthly_totals: Dict[int, float] = defaultdict(float, summaries["monthly_totals"])
    for adj in req.adjustments:
        monthly_totals[adj.month] += adj.delta
    accumulated = sum(monthly_totals.values())
    months_with_data = len([v for v in monthly_totals.values() if v > 0]) or dt.date.today().month
    forecast = (accumulated / months_with_data) * 12 if months_with_data else 0.0
    return _build_dashboard_payload(req.tenant_id, req.year, monthly_totals, accumulated, forecast)


@app.get("/limits/health")
def health():
    return {"ok": True}
