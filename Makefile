.PHONY: install lint format typecheck imports test check hooks up down clean logs ps migrate seed fixtures spacy-model drop demo

install:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff format .
	uv run ruff check --fix .

typecheck:
	uv run mypy --strict src/ecet/domain src/ecet/application
	uv run mypy src/ecet

imports:
	uv run lint-imports

test:
	uv run pytest

check: lint typecheck imports test

hooks:
	uv run pre-commit install

.env: | .env.example
	cp .env.example $@

DB_URL ?= postgresql+asyncpg://ecet:ecet@localhost:5432/ecet

migrate: .env
	ECET_DATABASE_URL=$(DB_URL) uv run alembic upgrade head

seed: .env
	ECET_DATABASE_URL=$(DB_URL) ECET_ENV=dev uv run ecet seed

up: .env
	docker compose up --build -d

down: .env
	docker compose down

clean: .env
	docker compose down -v

logs: .env
	docker compose logs -f api worker

ps: .env
	docker compose ps

fixtures:
	uv run python scripts/make_fixtures.py

spacy-model:
	uv run python -m spacy download $(or $(SPACY_MODEL),en_core_web_lg)

drop: fixtures
	./scripts/demo_drop.sh tests/fixtures/pdfs/note_simple.pdf $(or $(TENANT),tenant-a)

# `clean` removes the named volumes, including `rabbitdata`. That matters after a
# topology change: `claims.evaluate` is declared with `x-queue-type: quorum`, and a
# classic queue of the same name left in an old volume makes both the api and the
# worker fail to start with PRECONDITION_FAILED.

demo: fixtures up
	@echo "waiting for the api to become ready..."
	@for i in $$(seq 1 60); do \
		curl -sf localhost:8000/readyz >/dev/null && break; \
		[ $$i -eq 60 ] && { echo "api never became ready; try 'make logs'"; exit 1; }; \
		sleep 2; \
	done
	@# `minio-setup` restarts MinIO to pick up the webhook target. Dropping before it
	@# finishes either hits a refused connection or lands an object no event covers.
	@echo "waiting for minio-setup to finish wiring notifications..."
	@docker compose wait minio-setup
	@# The mock client keeps every delivery it has ever received. Without this, a second
	@# `make demo` without `make clean` leaves the previous run's delivery at `.[0]`.
	@curl -sf -X DELETE localhost:8081/received >/dev/null
	./scripts/demo_drop.sh tests/fixtures/pdfs/note_simple.pdf tenant-a
	./scripts/demo_drop.sh tests/fixtures/pdfs/note_unclear.pdf tenant-a
	./scripts/demo_drop.sh tests/fixtures/pdfs/note_simple.pdf tenant-empty || true
	@sleep 5
	docker compose logs --since 60s worker
	@echo "--- webhooks received by the mock client ---"
	curl -s localhost:8081/received | jq .
