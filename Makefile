.PHONY: install lint format typecheck imports test check hooks up down clean logs ps migrate seed fixtures spacy-model drop

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
