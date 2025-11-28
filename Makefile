DC ?= docker compose

.PHONY: up down test seed

up:
	$(DC) up -d --build

down:
	$(DC) down -v

test:
	$(DC) run --rm orchestrator pytest

seed:
	$(DC) run --rm orchestrator bash -lc "echo 'seed data pipeline placeholder'"
