from pathlib import Path

from tests.conftest import reload_module


def test_generate_documents_marks_errors(monkeypatch):
    monkeypatch.setenv("DEFAULT_TENANT_ID", "tenant-x")
    synthetic = reload_module("infra.synthetic_data")
    docs = synthetic.generate_synthetic_documents(count=20, error_ratio=0.2, seed=123)

    assert len(docs) == 20
    assert sum(1 for doc in docs if doc.has_error) == 4
    assert all(doc.tenant_id == "tenant-x" for doc in docs)
    assert any("NFSe" in doc.text or "RECIBO" in doc.text for doc in docs)


def test_upload_documents_with_filesystem_backend(tmp_path, monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "filesystem")
    monkeypatch.setenv("FILESYSTEM_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setenv("DEFAULT_TENANT_ID", "demo")
    synthetic = reload_module("infra.synthetic_data")
    storage = reload_module("documents.storage_s3")

    docs = synthetic.generate_synthetic_documents(count=3, error_ratio=0.1, seed=1)
    uploads = synthetic.upload_documents_via_presign(docs, tenant_id="demo")

    assert len(uploads) == 3
    for upload in uploads:
        stored = storage.get(upload["key"], tenant_id=upload["tenant_id"])
        assert stored
        assert Path(tmp_path / upload["storage_key"]).exists()
