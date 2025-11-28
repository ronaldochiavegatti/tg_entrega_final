import importlib
import importlib
import os
import sys
import types
import warnings
from pathlib import Path
from types import ModuleType
from typing import Iterable

import pytest


class DummyCursor:
    def __init__(self, fetchone_result=None, fetchall_results: Iterable = ()):  # type: ignore[assignment]
        self.statements = []
        self.fetchone_result = fetchone_result
        self.fetchall_results = list(fetchall_results)

    def execute(self, *args, **kwargs):
        self.statements.append((args, kwargs))

    def fetchone(self):
        return self.fetchone_result

    def fetchall(self):
        return self.fetchall_results

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyConn:
    def __init__(self, cursor: DummyCursor | None = None):
        self._cursor = cursor or DummyCursor()

    def cursor(self):
        return self._cursor

    def close(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@pytest.fixture(autouse=True)
def add_services_to_path(monkeypatch: pytest.MonkeyPatch) -> None:
    base = Path(__file__).resolve().parents[1]
    monkeypatch.syspath_prepend(str(base / "services"))


def _install_otel_stubs() -> None:
    if "opentelemetry" in sys.modules:
        return

    baggage = types.ModuleType("opentelemetry.baggage")
    baggage.set_baggage = lambda *args, **kwargs: None

    context = types.ModuleType("opentelemetry.context")
    context.get_current = lambda: None
    context.attach = lambda ctx: None
    context.detach = lambda token: None

    trace = types.ModuleType("opentelemetry.trace")
    trace.set_tracer_provider = lambda *args, **kwargs: None
    trace.get_tracer_provider = lambda: None

    exporter = types.ModuleType("opentelemetry.exporter")
    otlp = types.ModuleType("opentelemetry.exporter.otlp")
    proto = types.ModuleType("opentelemetry.exporter.otlp.proto")
    http = types.ModuleType("opentelemetry.exporter.otlp.proto.http")
    trace_exporter = types.ModuleType("opentelemetry.exporter.otlp.proto.http.trace_exporter")
    trace_exporter.OTLPSpanExporter = type("OTLPSpanExporter", (), {"__init__": lambda self, *a, **k: None})

    instr = types.ModuleType("opentelemetry.instrumentation")
    instr_fastapi = types.ModuleType("opentelemetry.instrumentation.fastapi")
    instr_fastapi.FastAPIInstrumentor = type(
        "FastAPIInstrumentor", (), {"instrument_app": staticmethod(lambda app: None)}
    )
    instr_requests = types.ModuleType("opentelemetry.instrumentation.requests")
    instr_requests.RequestsInstrumentor = type(
        "RequestsInstrumentor", (), {"instrument": staticmethod(lambda **kwargs: None)}
    )

    sdk = types.ModuleType("opentelemetry.sdk")
    sdk_resources = types.ModuleType("opentelemetry.sdk.resources")
    sdk_resources.Resource = type("Resource", (), {"create": staticmethod(lambda *a, **k: object())})
    sdk_trace = types.ModuleType("opentelemetry.sdk.trace")
    sdk_trace.TracerProvider = type(
        "TracerProvider",
        (),
        {"__init__": lambda self, *a, **k: None, "add_span_processor": lambda self, *a, **k: None},
    )
    sdk_trace_export = types.ModuleType("opentelemetry.sdk.trace.export")
    sdk_trace_export.BatchSpanProcessor = type(
        "BatchSpanProcessor", (), {"__init__": lambda self, *a, **k: None}
    )

    otel = types.ModuleType("opentelemetry")
    otel.baggage = baggage
    otel.context = context
    otel.trace = trace

    sys.modules.update(
        {
            "opentelemetry": otel,
            "opentelemetry.baggage": baggage,
            "opentelemetry.context": context,
            "opentelemetry.trace": trace,
            "opentelemetry.exporter": exporter,
            "opentelemetry.exporter.otlp": otlp,
            "opentelemetry.exporter.otlp.proto": proto,
            "opentelemetry.exporter.otlp.proto.http": http,
            "opentelemetry.exporter.otlp.proto.http.trace_exporter": trace_exporter,
            "opentelemetry.instrumentation": instr,
            "opentelemetry.instrumentation.fastapi": instr_fastapi,
            "opentelemetry.instrumentation.requests": instr_requests,
            "opentelemetry.sdk": sdk,
            "opentelemetry.sdk.resources": sdk_resources,
            "opentelemetry.sdk.trace": sdk_trace,
            "opentelemetry.sdk.trace.export": sdk_trace_export,
        }
    )


_install_otel_stubs()
warnings.filterwarnings("ignore", category=DeprecationWarning)
os.environ.setdefault("CAPTURE_LOG_WARNINGS", "0")


def reload_module(module_name: str) -> ModuleType:
    to_delete = [name for name in sys.modules if name == module_name or name.startswith(f"{module_name}.")]
    for name in to_delete:
        sys.modules.pop(name, None)
    return importlib.import_module(module_name)

