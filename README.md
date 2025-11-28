# Monorepo SaaS MEI

Estrutura básica para serviços FastAPI, gateway NGINX e frontend em um único repositório.

## Estrutura do projeto
- `services/auth`, `services/billing`, `services/documents`, `services/limits`, `services/orchestrator`: serviços FastAPI.
- `gateway/`: NGINX atuando como API Gateway em `8080`.
- `frontend/`: ponto de partida para a interface web.
- `infra/`: artefatos de infraestrutura e automações.
- `docker-compose.yml`: orquestração local com PostgreSQL, MongoDB e Redis.
- `Makefile`: comandos rápidos para subir, derrubar, testar e semear dados.

## Configuração de ambiente
1. Copie o arquivo de exemplo e ajuste credenciais Oracle Object Storage e demais variáveis:
   ```bash
   cp .env.example .env
   ```
2. Gere os pares RSA para o auth e atualize os caminhos em `.env`:
   ```bash
   openssl genrsa -out jwt_private.pem 2048
   openssl rsa -in jwt_private.pem -pubout -out jwt_public.pem
   ```

## Como rodar
```bash
make up
```

Healthchecks úteis:
- Auth: http://localhost:8081/auth/health
- Documents: http://localhost:8083/documents/health
- Limits: http://localhost:8087/limits/health
- Orchestrator: http://localhost:8085/orchestrator/health
- Billing: http://localhost:8086/billing/health
- Gateway: http://localhost:8080

Para encerrar o ambiente: `make down`.
