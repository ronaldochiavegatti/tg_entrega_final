import csv
import datetime as dt
import io
import logging
from typing import Dict, Iterable, List, Optional

from celery import Celery
from fastapi import Body, FastAPI, Header, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

from .db import get_conn

logger = logging.getLogger("billing")


class Settings(BaseSettings):
    postgres_dsn: str = Field(
        default="postgresql://postgres:postgres@postgres:5432/app",
        alias="POSTGRES_DSN",
    )
    redis_url: str = Field(default="redis://redis:6379/0", alias="REDIS_URL")
    default_tenant: str = Field(default="demo", alias="DEFAULT_TENANT_ID")
    trial_mode: bool = Field(default=True, alias="BILLING_TRIAL_MODE")


SUPPORTED_KINDS = {"rag", "ocr", "extract"}
DEFAULT_UNIT_PRICE = 0.000001

settings = Settings()
celery_app = Celery("billing", broker=settings.redis_url, backend=settings.redis_url)
pc_conn = get_conn()

app = FastAPI(title="billing")


class Tariff(BaseModel):
    kind: str = Field(pattern="^(rag|ocr|extract)$")
    unit_price: float = Field(gt=0)


class TariffResponse(BaseModel):
    tariffs: List[Tariff]


class BudgetRequest(BaseModel):
    tenant_id: str
    budget: float = Field(gt=0)
    alert_threshold: float = Field(default=0.8, gt=0, le=1.0)


class BudgetResponse(BaseModel):
    tenant_id: str
    budget: float
    alert_threshold: float
    alert_sent: bool


@app.on_event("startup")
def on_startup() -> None:
    _ensure_tables()


@app.on_event("shutdown")
def on_shutdown() -> None:
    pc_conn.close()


def _ensure_tables() -> None:
    with pc_conn.cursor() as cur:
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
                tokens_prompt INT NOT NULL DEFAULT 0,
                tokens_completion INT NOT NULL DEFAULT 0,
                cost NUMERIC(12,6) NOT NULL DEFAULT 0,
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
            [DEFAULT_UNIT_PRICE, DEFAULT_UNIT_PRICE, DEFAULT_UNIT_PRICE],
        )


def _publish_event(event_name: str, payload: Dict[str, object]) -> None:
    try:
        celery_app.send_task(event_name, kwargs={"payload": payload})
    except Exception as exc:  # pragma: no cover - broker optional
        logger.warning("Failed to publish event %s: %s", event_name, exc)


def _parse_ts(value: str, name: str) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=dt.timezone.utc)
        return parsed
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid {name}") from exc


def _load_tariffs() -> List[Tariff]:
    with pc_conn.cursor() as cur:
        cur.execute("SELECT kind, unit_price FROM billing_tariffs ORDER BY kind")
        rows = cur.fetchall()
    return [Tariff(kind=row[0], unit_price=float(row[1])) for row in rows]


def _get_budget(tenant_id: Optional[str]) -> Optional[BudgetResponse]:
    if not tenant_id:
        return None
    with pc_conn.cursor() as cur:
        cur.execute(
            "SELECT tenant_id, budget, alert_threshold, alert_sent FROM billing_budgets WHERE tenant_id = %s",
            [tenant_id],
        )
        row = cur.fetchone()
    if not row:
        return None
    return BudgetResponse(
        tenant_id=row[0],
        budget=float(row[1]),
        alert_threshold=float(row[2]),
        alert_sent=bool(row[3]),
    )


def _update_budget_alert(tenant_id: str) -> None:
    with pc_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE billing_budgets
            SET alert_sent = TRUE, last_alert_at = NOW(), updated_at = NOW()
            WHERE tenant_id = %s
            """,
            [tenant_id],
        )


def _apply_budget_alert(
    tenant_id: Optional[str], total_cost: float, budget: Optional[BudgetResponse], frm: Optional[str], to: Optional[str]
) -> Optional[bool]:
    if not tenant_id or not budget:
        return None
    threshold_value = budget.budget * budget.alert_threshold
    alert_triggered = total_cost >= threshold_value
    if alert_triggered and not budget.alert_sent:
        _publish_event(
            "billing_budget_alert",
            {
                "tenant_id": tenant_id,
                "budget": budget.budget,
                "threshold": budget.alert_threshold,
                "total_cost": total_cost,
                "from": frm,
                "to": to,
            },
        )
        _update_budget_alert(tenant_id)
    return alert_triggered


@app.post("/billing/tariffs", response_model=TariffResponse, status_code=status.HTTP_201_CREATED)
def set_tariffs(tariffs: List[Tariff] = Body(..., embed=True)):
    if not tariffs:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No tariffs provided")
    kinds = {tariff.kind for tariff in tariffs}
    unsupported = kinds - SUPPORTED_KINDS
    if unsupported:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported kinds: {', '.join(sorted(unsupported))}")

    with pc_conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO billing_tariffs (kind, unit_price)
            VALUES (%s, %s)
            ON CONFLICT (kind) DO UPDATE SET unit_price = EXCLUDED.unit_price
            """,
            [(tariff.kind, tariff.unit_price) for tariff in tariffs],
        )
    return TariffResponse(tariffs=_load_tariffs())


@app.get("/billing/tariffs", response_model=TariffResponse)
def get_tariffs():
    return TariffResponse(tariffs=_load_tariffs())


@app.post("/billing/budgets", response_model=BudgetResponse, status_code=status.HTTP_201_CREATED)
def upsert_budget(req: BudgetRequest):
    with pc_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO billing_budgets (tenant_id, budget, alert_threshold, alert_sent)
            VALUES (%s, %s, %s, FALSE)
            ON CONFLICT (tenant_id) DO UPDATE
                SET budget = EXCLUDED.budget,
                    alert_threshold = EXCLUDED.alert_threshold,
                    alert_sent = FALSE,
                    updated_at = NOW()
            RETURNING tenant_id, budget, alert_threshold, alert_sent
            """,
            [req.tenant_id, req.budget, req.alert_threshold],
        )
        row = cur.fetchone()
    return BudgetResponse(
        tenant_id=row[0], budget=float(row[1]), alert_threshold=float(row[2]), alert_sent=bool(row[3])
    )


@app.get("/billing/budgets/{tenant_id}", response_model=BudgetResponse)
def get_budget(tenant_id: str):
    budget = _get_budget(tenant_id)
    if not budget:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Budget not found")
    return budget


def _build_usage_rows(
    frm: Optional[str],
    to: Optional[str],
    tenant_id: Optional[str],
) -> tuple[List[List[object]], Dict[str, float]]:
    clauses: List[str] = []
    params: List[object] = []

    if tenant_id:
        clauses.append("tenant_id = %s")
        params.append(tenant_id)
    if frm:
        parsed_from = _parse_ts(frm, "from")
        clauses.append("ts >= %s")
        params.append(parsed_from)
    if to:
        parsed_to = _parse_ts(to, "to")
        clauses.append("ts <= %s")
        params.append(parsed_to)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows: List[List[object]] = []
    with pc_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DATE(ts) as day, kind,
                   SUM(tokens_prompt) as tokens_prompt,
                   SUM(tokens_completion) as tokens_completion,
                   SUM(cost) as cost
            FROM billing_usage
            {where}
            GROUP BY day, kind
            ORDER BY day, kind
            """,
            params,
        )
        for day, kind, tok_p, tok_c, cost in cur.fetchall():
            rows.append([day.isoformat(), kind, int(tok_p or 0), int(tok_c or 0), float(cost or 0)])

        cur.execute(
            f"SELECT COALESCE(SUM(tokens_prompt),0), COALESCE(SUM(tokens_completion),0), COALESCE(SUM(cost),0) FROM billing_usage {where}",
            params,
        )
        totals_row = cur.fetchone() or (0, 0, 0)

    totals = {
        "tokens_prompt": int(totals_row[0] or 0),
        "tokens_completion": int(totals_row[1] or 0),
        "cost": float(totals_row[2] or 0),
    }
    return rows, totals


@app.get("/billing/usage")
def usage(
    frm: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None, alias="to"),
    tenant_header: Optional[str] = Header(None, alias="X-Tenant-Id"),
    tenant_id: Optional[str] = Query(None, alias="tenant_id"),
):
    selected_tenant = tenant_id or tenant_header or settings.default_tenant
    rows, totals = _build_usage_rows(frm, to, selected_tenant)
    budget = _get_budget(selected_tenant)
    alert = _apply_budget_alert(selected_tenant, totals["cost"], budget, frm, to)

    def iter_rows() -> Iterable[str]:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["date", "kind", "tokens_prompt", "tokens_completion", "cost", "budget", "alert_triggered"])
        for day, kind, tok_p, tok_c, cost in rows:
            writer.writerow([day, kind, tok_p, tok_c, f"{cost:.6f}", "", ""])
        writer.writerow([
            "TOTAL",
            "",
            totals["tokens_prompt"],
            totals["tokens_completion"],
            f"{totals['cost']:.6f}",
            budget.budget if budget else "",
            "YES" if alert else "NO" if alert is not None else "",
        ])
        yield buffer.getvalue()

    headers = {"Content-Disposition": "attachment; filename=billing_usage.csv"}
    return StreamingResponse(iter_rows(), media_type="text/csv", headers=headers)


@app.get("/billing/health")
def health():
    return {"ok": True, "trial_mode": settings.trial_mode}
