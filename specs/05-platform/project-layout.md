# Platform: Project Layout

```
.
├── README.md
├── pyproject.toml              # uv; deps, ruff, mypy, pytest config
├── uv.lock
├── Dockerfile                  # single image, entrypoint `ecet`
├── docker-compose.yml
├── .env.example
├── Makefile                    # up, down, test, lint, seed, demo
├── migrations/                 # alembic
├── scripts/
│   ├── make_fixtures.py        # generate PDF fixtures (reportlab)
│   └── demo_drop.sh            # mc cp fixture → bucket, tail logs
├── services/mock-client/       # tiny FastAPI webhook receiver (own Dockerfile)
├── specs/                      # this folder ([index](../00-overview.md#5-spec-index))
├── src/ecet/
│   ├── __init__.py
│   ├── cli.py                  # typer: api | worker | seed | dlq-replay
│   ├── config.py               # Settings (pydantic-settings)
│   ├── domain/
│   │   ├── claim.py  policy.py  evaluation.py  tenant.py  rules.py  errors.py
│   │   └── ports/  (claim_repository.py policy_repository.py tenant_repository.py review_task_repository.py)
│   ├── application/
│   │   ├── ports/  (object_storage.py text_extractor.py pii_redactor.py evaluation_queue.py llm_gateway.py webhook_client.py clock.py unit_of_work.py)
│   │   ├── use_cases/ (ingest_claim_document.py redact_pii.py attach_tenant_policies.py run_deterministic_checks.py enqueue_evaluation.py evaluate_claim.py route_decision.py notify_client.py human_review.py)
│   │   ├── messages.py  notifications.py  redaction_policy.py
│   │   └── prompts/evaluate_v1.py
│   ├── infrastructure/
│   │   ├── storage/s3.py
│   │   ├── pdf/pypdf_extractor.py
│   │   ├── pii/presidio_redactor.py  pii/fake_redactor.py
│   │   ├── postgres/ (orm.py mappers.py repositories.py unit_of_work.py seed/)
│   │   ├── queue/rabbitmq.py  queue/in_memory.py
│   │   ├── llm/ (openai_gateway.py fake_gateway.py factory.py)
│   │   ├── webhook/httpx_client.py  webhook/fake_client.py
│   │   └── observability/ (logging.py metrics.py)
│   └── interfaces/
│       ├── api/ (app.py container.py errors.py routes/…)
│       └── worker/ (main.py container.py handler.py)
└── tests/
    ├── unit/          (domain, use cases with fakes — fast, no docker)
    ├── adapters/      (each infra adapter, testcontainers, marked slow)
    ├── api/           (FastAPI with fake ports)
    ├── e2e/           (compose-level: drop PDF → webhook received)
    └── fixtures/      (pdfs/, notes/, s3_events/)
```

## Import boundaries (enforced by `import-linter` contract in pyproject)
- `ecet.domain` imports: stdlib, pydantic only.
- `ecet.application` imports: domain + stdlib + pydantic.
- `ecet.infrastructure` may import application + domain.
- `ecet.interfaces` may import everything.
- Nothing imports `ecet.interfaces` except `cli.py`.

## Tooling
- `ruff` (lint + format), `mypy --strict` on domain/application, `import-linter`, `pytest -m "not slow"` default.
- `pre-commit` with the above.
- CI (GitHub Actions): lint, type, unit; nightly/`slow` job runs adapters + e2e with Docker.
