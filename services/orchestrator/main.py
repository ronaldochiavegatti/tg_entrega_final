from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="orchestrator")


class ChatReq(BaseModel):
    message: str
    tools_allowed: list[str] | None = None


@app.post("/chat")
def chat(req: ChatReq):
    # TODO: integrar RAG/LLM e contagem de tokens
    usage = {"tokens_prompt": 10, "tokens_completion": 20, "cost": 0.001}
    return {"reply": "(stub) resposta do agente", "usage": usage}


@app.get("/orchestrator/health")
def health():
    return {"ok": True}
