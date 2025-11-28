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

## CI/CD
- CI executa `ruff`, testes unitários com `pytest`, sobe o stack com `docker compose` para checagem de integração e então constrói as imagens Docker dos serviços FastAPI. Cada imagem é analisada por Trivy e Grype antes de seguir adiante.
- Tags no formato `v*` disparam a publicação das imagens no GHCR e um deploy automático para o ambiente de staging. Há um estágio separado que exige aprovação manual para promover a mesma tag para produção.

### Segredos no pipeline
- Armazene chaves sensíveis no provedor de segredos do CI (ex.: GitHub Secrets) e não no repositório. O pipeline espera encontrar `ORACLE_S3_ACCESS_KEY_ID`, `ORACLE_S3_SECRET_ACCESS_KEY`, `JWT_PRIVATE_KEY` e `JWT_PUBLIC_KEY` como segredos.
- Os secrets são materializados em arquivos e variáveis somente durante os jobs de deploy (staging/produção), evitando que chaves Oracle ou JWT sejam versionadas ou exibidas em logs.
