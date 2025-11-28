import hashlib
import json
import time
from pathlib import Path
from typing import Dict, List, Optional

import jwt
from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from opentelemetry import baggage, context, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    jwt_alg: str = "RS256"
    jwt_private_key_path: str = "jwt_private.pem"
    jwt_public_key_path: str = "jwt_public.pem"
    access_ttl_seconds: int = 3600
    refresh_ttl_seconds: int = 60 * 60 * 24 * 7
    user_store_path: str = "data/users.json"
    default_tenant: str = "demo"


settings = Settings()


def _load_key(path: str) -> str:
    key_path = Path(path)
    if not key_path.exists():
        raise RuntimeError(f"JWT key not found at {key_path}")
    return key_path.read_text()


PRIVATE_KEY = _load_key(settings.jwt_private_key_path)
PUBLIC_KEY = _load_key(settings.jwt_public_key_path)

USER_STORE_PATH = Path(settings.user_store_path)
if not USER_STORE_PATH.exists():
    raise RuntimeError(f"User store not found at {USER_STORE_PATH}")
USERS: List[Dict[str, str]] = json.loads(USER_STORE_PATH.read_text()).get("users", [])
USERS_BY_EMAIL = {user["email"]: user for user in USERS}

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


app = FastAPI(title="auth")
_configure_observability(app, "auth")
security = HTTPBearer(auto_error=False)


class LoginPayload(BaseModel):
    email: str
    password: str


class RefreshPayload(BaseModel):
    refresh_token: str


def hash_password(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def _base_claims(user: Dict[str, str]) -> Dict[str, str]:
    return {
        "sub": user["id"],
        "email": user["email"],
        "role": user.get("role", "viewer"),
        "tenant_id": user.get("tenant_id") or settings.default_tenant,
    }


def encode_token(payload: Dict[str, str], ttl_seconds: int) -> str:
    now = int(time.time())
    claims = {
        **payload,
        "iat": now,
        "exp": now + ttl_seconds,
    }
    return jwt.encode(claims, PRIVATE_KEY, algorithm=settings.jwt_alg)


def decode_token(token: str, expected_type: str = "access") -> Dict[str, str]:
    try:
        data = jwt.decode(token, PUBLIC_KEY, algorithms=[settings.jwt_alg])
        if data.get("type") != expected_type:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token type")
        return data
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="token expired")
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token") from exc


def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
):
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing bearer token")
    return decode_token(credentials.credentials, expected_type="access")


def role_required(allowed_roles: List[str]):
    def checker(user=Depends(get_current_user)):
        if user.get("role") not in allowed_roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="forbidden")
        return user

    return checker


@app.post("/auth/login")
def login(payload: LoginPayload):
    user = USERS_BY_EMAIL.get(payload.email)
    if not user or user.get("password_hash") != hash_password(payload.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid credentials")

    base = _base_claims(user)
    access_token = encode_token({**base, "type": "access"}, settings.access_ttl_seconds)
    refresh_token = encode_token({**base, "type": "refresh"}, settings.refresh_ttl_seconds)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "expires_in": settings.access_ttl_seconds,
    }


@app.post("/auth/refresh")
def refresh(payload: RefreshPayload):
    data = decode_token(payload.refresh_token, expected_type="refresh")
    base = {k: data[k] for k in ["sub", "email", "role", "tenant_id"] if k in data}
    new_access = encode_token({**base, "type": "access"}, settings.access_ttl_seconds)
    return {
        "access_token": new_access,
        "token_type": "bearer",
        "expires_in": settings.access_ttl_seconds,
    }


@app.get("/auth/validate")
def validate(user=Depends(get_current_user)):
    headers = {
        "X-User-Id": user.get("sub", ""),
        "X-Tenant-Id": user.get("tenant_id", ""),
    }
    return JSONResponse({"user": user}, headers=headers)


@app.get("/auth/me")
def me(user=Depends(role_required(["viewer", "editor", "admin"]))):
    return {"user": user}


@app.get("/auth/health")
def health(request: Request):
    return {"ok": True, "path": request.url.path}
