from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional, List

from .storage_s3 import DEFAULT_TENANT, presign_get, presign_put

app = FastAPI(title="documents")


def _resolve_tenant(tenant_id: Optional[str]) -> str:
    return tenant_id or DEFAULT_TENANT


class PresignUploadReq(BaseModel):
    key: str
    content_type: Optional[str] = "application/pdf"
    tenant_id: Optional[str] = None


@app.post("/storage/presign-upload")
def presign_upload(req: PresignUploadReq):
    tenant = _resolve_tenant(req.tenant_id)
    data = presign_put(req.key, req.content_type, tenant)
    return {"url": data["url"], "expires_in": data["expires_in"], "key": data["key"]}


@app.get("/storage/presign-download")
def presign_download(key: str, tenant_id: Optional[str] = None):
    tenant = _resolve_tenant(tenant_id)
    data = presign_get(key, tenant)
    return {"url": data["url"], "expires_in": data["expires_in"], "key": data["key"]}


# --- CRUD mínimo (mock) ---
class PatchChange(BaseModel):
    path: str
    value: object


@app.get("/documents/{doc_id}")
def get_doc(doc_id: str):
    return {"_id": doc_id, "totals": {"gross_amount": 0}, "fields": []}


@app.patch("/documents/{doc_id}")
def patch_doc(doc_id: str, changes: List[PatchChange]):
    # TODO: validar e persistir; publicar FIELDS_UPDATED
    return {"doc_id": doc_id, "updated": len(changes)}


@app.get("/documents/{doc_id}/audit")
def audit(doc_id: str):
    return {"doc_id": doc_id, "audit": []}


@app.get("/documents/health")
def health():
    return {"ok": True}
