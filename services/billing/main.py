from fastapi import FastAPI
from typing import Optional

app = FastAPI(title="billing")


@app.get("/billing/usage")
def usage(frm: Optional[str] = None, to: Optional[str] = None):
    # TODO: agregar de fato a partir de tabelas
    return {"from": frm, "to": to, "rows": []}


@app.get("/billing/health")
def health():
    return {"ok": True}
