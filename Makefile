.PHONY: install lint format typecheck imports test check up down clean logs ps

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


.env: | .env.example
	cp .env.example $@

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
