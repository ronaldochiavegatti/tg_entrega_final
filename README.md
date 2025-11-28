# MVP SaaS MEI — Esqueletos mínimos

## Subir
```bash
cp .env.example .env
# gere seus pares RSA para o auth (exemplo rápido)
openssl genrsa -out jwt_private.pem 2048
openssl rsa -in jwt_private.pem -pubout -out jwt_public.pem
# mova para um diretório seguro e aponte via .env
make up
```

Testar healthchecks
Auth: http://localhost:8081/auth/health
Documents: http://localhost:8083/documents/health
Limits: http://localhost:8087/limits/health
Orchestrator: http://localhost:8085/orchestrator/health
Billing: http://localhost:8086/billing/health
Gateway: http://localhost:8080
