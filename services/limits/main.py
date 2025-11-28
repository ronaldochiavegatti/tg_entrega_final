from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from .db import get_conn

app = FastAPI(title="limits")


class RecalcReq(BaseModel):
    tenant_id: str
    year: int
    doc_ids: Optional[list[str]] = None


@app.get("/limits/{year}/dashboard")
def dashboard(year: int, tenant_id: str):
    # TODO: consultar snapshots; mock inicial
    return {
        "accumulated": 0.0,
        "forecast": 0.0,
        "state": "OK",
        "threshold": {"warn": 0.8, "critical": 1.0},
        "months": [{"m": i, "value": 0.0} for i in range(1, 13)]
    }


@app.post("/limits/recalculate")
def recalc(body: RecalcReq):
    # TODO: consumir eventos e recalcular; mock
    return {"accepted": True}


@app.get("/limits/health")
def health():
    return {"ok": True}
