from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from .storage_s3 import presign_put, presign_get

app = FastAPI(title="documents")


class PresignUploadReq(BaseModel):
    key: str
    content_type: Optional[str] = "application/pdf"


@app.post("/storage/presign-upload")
def presign_upload(req: PresignUploadReq):
    data = presign_put(req.key, req.content_type)
    return {"url": data["url"], "fields": data["fields"], "expires_in": 900}


@app.get("/storage/presign-download")
def presign_download(key: str):
    return {"url": presign_get(key), "expires_in": 900}


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
