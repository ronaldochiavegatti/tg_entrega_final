from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import time, jwt, os

ALG = os.getenv("JWT_ALG", "RS256")
PRIV = os.getenv("JWT_PRIVATE_KEY_PATH", "jwt_private.pem")
PUB = os.getenv("JWT_PUBLIC_KEY_PATH", "jwt_public.pem")

with open(PRIV, "r") as f:
    PRIVATE_KEY = f.read()
with open(PUB, "r") as f:
    PUBLIC_KEY = f.read()

app = FastAPI(title="auth")


class Login(BaseModel):
    email: str
    password: str


@app.post("/auth/login")
def login(payload: Login):
    if payload.email == "admin@demo.local" and payload.password == "admin":
        now = int(time.time())
        access = jwt.encode({"sub": payload.email, "role": "admin", "iat": now, "exp": now + 3600}, PRIVATE_KEY, algorithm=ALG)
        return {"access_token": access, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="invalid credentials")


@app.get("/auth/health")
def health():
    return {"ok": True}
