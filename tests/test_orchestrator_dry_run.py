import psycopg

from tests.conftest import DummyConn, reload_module


def test_chat_responds_in_dry_run(monkeypatch):
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("DEFAULT_TENANT_ID", "tenant-ci")
    monkeypatch.setattr(psycopg, "connect", lambda *a, **k: DummyConn())

    orchestrator = reload_module("orchestrator.main")
    req = orchestrator.ChatReq(message="preciso corrigir limite", tools_allowed=["corrigir_campo"])
    resp = orchestrator.chat(req)

    assert resp.get("dry_run") is True
    assert resp["usage"].get("dry_run") is True
    assert resp["usage"]["tokens_prompt"] > 0
    assert resp["tool_calls"][0]["name"] == "corrigir_campo"
    assert any("DRY_RUN" in line or "Modo" in line for line in resp["reply"].split("\n"))


def test_search_flags_dry_run(monkeypatch):
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("DEFAULT_TENANT_ID", "tenant-ci")
    monkeypatch.setattr(psycopg, "connect", lambda *a, **k: DummyConn())

    orchestrator = reload_module("orchestrator.main")
    result = orchestrator.search("tokenizacao em CI", tenant_id="tenant-ci")

    assert result.get("dry_run") is True
    assert result["results"]
    assert all(item.get("score", 0) == 1.0 for item in result["results"])
