# Monorepo SaaS MEI

Base unificada para serviços FastAPI, gateway NGINX e frontend. O repositório oferece tudo que é necessário para desenvolver, testar e colocar a aplicação em produção com Docker.

## Visão geral
- **Serviços FastAPI**: `services/auth`, `services/billing`, `services/documents`, `services/limits`, `services/orchestrator`.
- **Gateway**: `gateway/` expõe o tráfego HTTP na porta `8080` e encaminha para os serviços.
- **Frontend**: código inicial em `frontend/` para o painel web.
- **Infra**: automações, scripts e artefatos em `infra/` (demo, carga, secrets, etc.).
- **Orquestração**: `docker-compose.yml` levanta PostgreSQL, MongoDB, Redis e os serviços.
- **Makefile**: atalhos para subir/derrubar o stack, rodar testes e popular dados sintéticos.

## Pré-requisitos locais
- Docker e Docker Compose v2 instalados e configurados.
- `make` disponível no PATH.
- `openssl` para gerar chaves RSA usadas pelo serviço de autenticação.

## Configuração de ambiente
1. Crie seu arquivo de variáveis copiando o exemplo:
   ```bash
   cp .env.example .env
   ```
2. Gere os pares RSA para o auth e aponte os caminhos em `.env` (`JWT_PRIVATE_KEY_PATH` e `JWT_PUBLIC_KEY_PATH`):
   ```bash
   openssl genrsa -out jwt_private.pem 2048
   openssl rsa -in jwt_private.pem -pubout -out jwt_public.pem
   ```
3. Preencha as credenciais do Oracle Object Storage (ou ajuste `STORAGE_BACKEND=filesystem` para desenvolvimento local) e defina o `DEFAULT_TENANT_ID` conforme necessário.

> O `.env` é carregado pelo `docker-compose.yml` e compartilhado entre os serviços; mantenha credenciais fora do repositório.

## Executando localmente
1. Construa e suba todo o stack em segundo plano:
   ```bash
   make up
   ```
2. Acompanhe os healthchecks para confirmar que os serviços responderam:
   - Auth: http://localhost:8081/auth/health
   - Documents: http://localhost:8083/documents/health
   - Limits: http://localhost:8087/limits/health
   - Orchestrator: http://localhost:8085/orchestrator/health
   - Billing: http://localhost:8086/billing/health
   - Gateway: http://localhost:8080
3. Para encerrar e limpar volumes: `make down`.

### Dados de demonstração e fluxo completo
- Popular dados sintéticos para testes rápidos:
  ```bash
  make seed
  ```
- Rodar o fluxo presign → upload → `PATCH` → recalcular limites sem dependências externas:
  ```bash
  DEFAULT_TENANT_ID=demo STORAGE_BACKEND=filesystem python infra/demo_flow.py
  ```
  O script salva o arquivo em `.demo_storage/`, registra audit trail e valida o SLA (≤5s).
- Testes de carga prontos em `infra/load/` (k6 e Locust) cobrindo `/storage/presign-upload`, `PATCH /documents/{id}` e `/limits/recalculate`.

## Testes antes de ir para produção
- **Unitários/internos** (executados no container do `orchestrator`):
  ```bash
  make test
  ```
  O alvo roda `pytest` com a suíte de integração e lógica de limites localizada em `tests/`.
- **Auditoria manual**: validar CSP em modo **enforce** via gateway (somente `self`, `ws/wss` e `data:` para imagens) para evitar dependência de assets externos.
- **Carga**: usar os cenários de k6/Locust em `infra/load/` para avaliar `/storage/presign-upload`, `PATCH /documents/{id}` e `/limits/recalculate` antes de promover uma tag.

## Deploy e pipeline
- O CI executa `ruff`, `pytest`, sobe o stack com `docker compose` para testes de integração e gera as imagens Docker dos serviços FastAPI, analisando-as com Trivy e Grype.
- Tags `v*` publicam imagens no GHCR e disparam deploy automático para **staging**. A promoção para **produção** requer aprovação manual.

### Segredos no pipeline
- Armazene chaves sensíveis no provedor de segredos do CI (ex.: GitHub Secrets). O pipeline espera `ORACLE_S3_ACCESS_KEY_ID`, `ORACLE_S3_SECRET_ACCESS_KEY`, `JWT_PRIVATE_KEY` e `JWT_PUBLIC_KEY`.
- Os secrets são materializados apenas durante os jobs de deploy (staging/produção) para evitar exposição de credenciais Oracle ou JWT.
