import hashlib
import json
import logging
import re
import time
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.encoders import jsonable_encoder
from opentelemetry import baggage, context, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, REGISTRY, generate_latest
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from pymongo import MongoClient

from common.auth import require_roles
from common.logging import configure_structured_logging

configure_structured_logging("orchestrator")
logger = logging.getLogger("orchestrator")


class Settings(BaseSettings):
    postgres_dsn: str = Field(
        default="postgresql://postgres:postgres@postgres:5432/app",
        alias="POSTGRES_DSN",
    )
    mongo_uri: str = Field(default="mongodb://mongo:27017", alias="MONGO_URI")
    mongo_db: str = Field(default="app", alias="MONGO_DB")
    default_tenant: str = Field(default="demo", alias="DEFAULT_TENANT_ID")
    embedding_dim: int = Field(default=64, alias="EMBEDDING_DIM")
    chunk_size: int = Field(default=800, alias="CHUNK_SIZE")
    chunk_overlap: int = Field(default=120, alias="CHUNK_OVERLAP")
    dry_run: bool = Field(default=False, alias="DRY_RUN")


settings = Settings()


class _DummyCursor:
    def __init__(self):
        self.queries: list[tuple] = []

    def execute(self, *args, **kwargs):
        self.queries.append((args, kwargs))

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyConn:
    def cursor(self):
        return _DummyCursor()

    def close(self):
        return None


class _DummyCollection(dict):
    def find_one(self, _query):
        return None

    def find(self, _query):
        return []


if settings.dry_run:
    pg_conn = _DummyConn()
    mongo_client = None
    mongo_db = None
    documents_collection = _DummyCollection()
else:
    pg_conn = psycopg.connect(settings.postgres_dsn, autocommit=True)
    mongo_client = MongoClient(settings.mongo_uri)
    mongo_db = mongo_client[settings.mongo_db]
    documents_collection = mongo_db["documents"]

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
    BILLING_TOKENS = Counter(
        "billing_tokens",
        "Total billing tokens recorded",
        ["tenant", "kind", "direction"],
    )
    BILLING_COST = Counter(
        "billing_cost",
        "Accumulated billing cost", ["tenant", "kind"]
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
    BILLING_TOKENS = collectors.get("billing_tokens") or Counter(
        "billing_tokens",
        "Total billing tokens recorded",
        ["tenant", "kind", "direction"],
        registry=None,
    )
    BILLING_COST = collectors.get("billing_cost") or Counter(
        "billing_cost",
        "Accumulated billing cost", ["tenant", "kind"], registry=None
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


app = FastAPI(title="orchestrator")
_configure_observability(app, "orchestrator")


def _ensure_tables() -> None:
    if settings.dry_run:
        return
    with pg_conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS embeddings (
                id BIGSERIAL PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                chunk_no INT NOT NULL,
                text TEXT NOT NULL,
                embedding VECTOR(%s) NOT NULL,
                meta JSONB DEFAULT '{}'::jsonb
            );
            CREATE INDEX IF NOT EXISTS idx_embeddings_tenant_doc_chunk
                ON embeddings(tenant_id, doc_id, chunk_no);
            CREATE INDEX IF NOT EXISTS idx_embeddings_vector
                ON embeddings USING hnsw (embedding vector_l2_ops);
            """,
            [settings.embedding_dim],
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS billing_tariffs (
                kind TEXT PRIMARY KEY,
                unit_price NUMERIC(12,6) NOT NULL
            );

            CREATE TABLE IF NOT EXISTS billing_usage (
                id BIGSERIAL PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                kind TEXT NOT NULL,
                tokens_prompt INT NOT NULL,
                tokens_completion INT NOT NULL,
                cost NUMERIC(12,6) NOT NULL,
                meta JSONB DEFAULT '{}'::jsonb
            );
            CREATE INDEX IF NOT EXISTS idx_billing_usage_tenant_ts
                ON billing_usage (tenant_id, ts DESC);
            CREATE INDEX IF NOT EXISTS idx_billing_usage_kind_ts
                ON billing_usage (kind, ts DESC);

            CREATE TABLE IF NOT EXISTS billing_budgets (
                tenant_id TEXT PRIMARY KEY,
                budget NUMERIC(12,2) NOT NULL,
                alert_threshold NUMERIC(5,2) NOT NULL DEFAULT 0.8,
                alert_sent BOOLEAN NOT NULL DEFAULT FALSE,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_alert_at TIMESTAMPTZ
            );
            """
        )
        cur.execute(
            """
            INSERT INTO billing_tariffs (kind, unit_price)
            VALUES ('rag', %s), ('ocr', %s), ('extract', %s)
            ON CONFLICT (kind) DO NOTHING
            """,
            [0.000001, 0.000001, 0.000001],
        )

@app.on_event("startup")
def on_startup() -> None:
    _ensure_tables()


@app.on_event("shutdown")
def on_shutdown() -> None:
    if mongo_client:
        mongo_client.close()
    if pg_conn:
        pg_conn.close()


def _tokenize(text: str) -> List[str]:
    normalized = re.sub(r"[^\w]+", " ", text)
    return [tok.lower() for tok in normalized.split() if tok.strip()]


def _fake_embedding(text: str) -> List[float]:
    digest = hashlib.sha256(text.encode()).digest()
    numbers = [b for b in digest]
    vec: List[float] = []
    for i in range(settings.embedding_dim):
        vec.append(numbers[i % len(numbers)] / 255.0)
    return vec


def _vector_literal(vec: Iterable[float]) -> str:
    return f"[{','.join(str(v) for v in vec)}]"


def _chunk_text(text: str) -> List[str]:
    chunks: List[str] = []
    size = settings.chunk_size
    overlap = settings.chunk_overlap
    start = 0
    while start < len(text):
        end = min(len(text), start + size)
        chunks.append(text[start:end])
        if end >= len(text):
            break
        start = max(0, end - overlap)
        if start >= end:
            break
    return chunks


def _dry_run_contexts(query: str, tenant_id: str) -> List["SearchResponse"]:
    base_text = query or "Contexto simulado para dry-run"
    chunks = _chunk_text(base_text) or [base_text]
    return [
        SearchResponse(
            doc_id=f"dry-run-{idx}",
            chunk_no=idx,
            text=chunk,
            tenant_id=tenant_id,
            score=1.0,
            meta={"dry_run": True},
        )
        for idx, chunk in enumerate(chunks[:3])
    ]


def _get_unit_price(kind: str) -> float:
    if settings.dry_run:
        return 0.0
    with pg_conn.cursor() as cur:
        cur.execute("SELECT unit_price FROM billing_tariffs WHERE kind = %s", [kind])
        row = cur.fetchone()
    return float(row[0]) if row else 0.000001


def _record_usage(
    tenant_id: str, tokens_prompt: int, tokens_completion: int, meta: Optional[Dict], kind: str = "rag"
) -> Dict[str, float]:
    unit_price = _get_unit_price(kind)
    cost = round((tokens_prompt + tokens_completion) * unit_price, 6)
    BILLING_TOKENS.labels(tenant_id, kind, "prompt").inc(tokens_prompt)
    BILLING_TOKENS.labels(tenant_id, kind, "completion").inc(tokens_completion)
    BILLING_COST.labels(tenant_id, kind).inc(cost)
    if settings.dry_run:
        return {
            "tokens_prompt": tokens_prompt,
            "tokens_completion": tokens_completion,
            "unit_price": unit_price,
            "cost": cost,
            "dry_run": True,
        }

    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO billing_usage (tenant_id, kind, tokens_prompt, tokens_completion, cost, meta)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            [tenant_id, kind, tokens_prompt, tokens_completion, cost, json.dumps(meta or {})],
        )
    return {
        "tokens_prompt": tokens_prompt,
        "tokens_completion": tokens_completion,
        "unit_price": unit_price,
        "cost": cost,
    }


class IngestReq(BaseModel):
    doc_id: str
    tenant_id: Optional[str] = None


@app.post("/ingest", dependencies=[Depends(require_roles(["editor", "admin"]))])
def ingest(req: IngestReq):
    if settings.dry_run:
        tenant = req.tenant_id or settings.default_tenant
        chunks = _chunk_text(f"Ingestao simulada para {req.doc_id}") or [req.doc_id]
        return {
            "doc_id": req.doc_id,
            "tenant_id": tenant,
            "chunks": len(chunks),
            "dry_run": True,
        }

    doc = documents_collection.find_one({"_id": req.doc_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    tenant_id = req.tenant_id or doc.get("tenant_id") or settings.default_tenant
    text = str(doc.get("storage", {}).get("mock_text") or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Document has no text to ingest")

    chunks = _chunk_text(text)
    embeddings: List[Tuple[str, str, int, str, str, str]] = []
    for idx, chunk in enumerate(chunks):
        embedding = _fake_embedding(chunk)
        meta = {"source": "ocr", "tenant_id": tenant_id, "doc_id": req.doc_id}
        embeddings.append(
            (
                tenant_id,
                req.doc_id,
                idx,
                chunk,
                _vector_literal(embedding),
                json.dumps(meta),
            )
        )

    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM embeddings WHERE tenant_id = %s AND doc_id = %s", [tenant_id, req.doc_id])
        cur.executemany(
            """
            INSERT INTO embeddings (tenant_id, doc_id, chunk_no, text, embedding, meta)
            VALUES (%s, %s, %s, %s, %s::vector, %s)
            """,
            embeddings,
        )

    return {"doc_id": req.doc_id, "tenant_id": tenant_id, "chunks": len(chunks)}


class SearchResponse(BaseModel):
    doc_id: str
    chunk_no: int
    text: str
    tenant_id: str
    score: float
    meta: Dict[str, object] = Field(default_factory=dict)


def _lexical_score(text: str, tokens: List[str]) -> float:
    chunk_tokens = set(_tokenize(text))
    if not tokens:
        return 0.0
    return len(chunk_tokens & set(tokens)) / len(set(tokens))


def hybrid_search(query: str, tenant_id: str, top_k: int = 8) -> List[SearchResponse]:
    if settings.dry_run:
        return _dry_run_contexts(query, tenant_id)
    query_vec = _vector_literal(_fake_embedding(query))
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, tenant_id, doc_id, chunk_no, text, meta, 1 - (embedding <=> %s::vector) AS similarity
            FROM embeddings
            WHERE tenant_id = %s
            ORDER BY embedding <-> %s::vector
            LIMIT %s
            """,
            [query_vec, tenant_id, query_vec, top_k * 3],
        )
        rows = cur.fetchall()

    tokens = _tokenize(query)
    reranked: List[Tuple[float, Tuple]] = []
    for row in rows:
        row_dict = {
            "id": row[0],
            "tenant_id": row[1],
            "doc_id": row[2],
            "chunk_no": row[3],
            "text": row[4],
            "meta": row[5] if isinstance(row[5], dict) else json.loads(row[5] or "{}"),
            "similarity": float(row[6] or 0.0),
        }
        rerank = 0.7 * row_dict["similarity"] + 0.3 * _lexical_score(row_dict["text"], tokens)
        reranked.append((rerank, row_dict))

    reranked.sort(key=lambda item: item[0], reverse=True)
    top = reranked[:top_k]
    return [
        SearchResponse(
            doc_id=row["doc_id"],
            chunk_no=row["chunk_no"],
            text=row["text"],
            tenant_id=row["tenant_id"],
            score=round(score, 4),
            meta=row["meta"],
        )
        for score, row in top
    ]


@app.get("/search")
def search(query: str, tenant_id: Optional[str] = None):
    tenant = tenant_id or settings.default_tenant
    results = hybrid_search(query, tenant)
    payload = {"query": query, "tenant_id": tenant, "results": [r.dict() for r in results]}
    if settings.dry_run:
        payload["dry_run"] = True
    return payload


class ChatReq(BaseModel):
    message: str
    tools_allowed: List[str] | None = None
    tenant_id: Optional[str] = None


def _maybe_call_tool(tools_allowed: Optional[List[str]], message: str) -> List[Dict[str, object]]:
    if not tools_allowed:
        return []
    lowered = message.lower()
    calls = []
    if "corrigir" in lowered and "corrigir_campo" in tools_allowed:
        calls.append({"name": "corrigir_campo", "result": "Campo corrigido com sucesso."})
    if "recalcular" in lowered and "recalcular_limites" in tools_allowed:
        calls.append({"name": "recalcular_limites", "result": "Limites recalculados."})
    if "limite" in lowered and "mostrar_limites" in tools_allowed:
        calls.append({"name": "mostrar_limites", "result": "Limites atuais exibidos."})
    return calls


@app.post("/chat")
def chat(req: ChatReq):
    tenant = req.tenant_id or settings.default_tenant
    contexts = hybrid_search(req.message, tenant)
    tool_calls = _maybe_call_tool(req.tools_allowed, req.message)

    context_snippets = " ".join(chunk.text for chunk in contexts[:3])
    reply_parts = [f"Contexto: {context_snippets}" if context_snippets else "Sem contexto relevante."]
    if tool_calls:
        reply_parts.append("Ferramentas acionadas: " + ", ".join(call["name"] for call in tool_calls))
    else:
        reply_parts.append("Nenhuma ferramenta necessária.")
    reply_parts.append(f"Resposta baseada na mensagem: {req.message}")
    if settings.dry_run:
        reply_parts.append("Modo DRY_RUN: tokens e chamadas simulados.")
    reply = " \n".join(reply_parts)

    prompt_tokens = len(_tokenize(req.message)) + sum(len(_tokenize(c.text)) for c in contexts)
    completion_tokens = len(_tokenize(reply))
    usage = _record_usage(
        tenant,
        tokens_prompt=prompt_tokens,
        tokens_completion=completion_tokens,
        meta={"tool_calls": tool_calls, "contexts": jsonable_encoder(contexts)},
        kind="rag",
    )

    response = {
        "reply": reply,
        "context": [c.dict() for c in contexts],
        "tool_calls": tool_calls,
        "usage": usage,
    }

    if settings.dry_run:
        response["dry_run"] = True

    return response


@app.get("/orchestrator/health")
def health():
    return {"ok": True}
