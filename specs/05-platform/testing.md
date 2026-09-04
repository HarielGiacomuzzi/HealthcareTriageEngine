# Platform: Testing Strategy

## Layers
| Layer     | Location          | Deps                 | Marker | Runs in |
|-----------|-------------------|----------------------|--------|---------|
| Domain    | `tests/unit/domain`      | none          | —      | every commit |
| Use cases | `tests/unit/application` | fakes in `tests/fakes.py` | — | every commit |
| API       | `tests/api`       | FastAPI + fakes      | —      | every commit |
| Adapters  | `tests/adapters`  | testcontainers (pg, rabbit, minio), real presidio | `slow` | CI nightly + pre-merge |
| E2E       | `tests/e2e`       | full compose, fake LLM | `e2e` | manual / release |

## Fakes (`tests/fakes.py`) — one per [port](../02-use-cases/README.md#ports-defined-in-ecetapplicationports), in-memory
`FakeClaimRepository`, `FakePolicyRepository`, `FakeTenantRepository`, `FakeReviewTaskRepository`,
`FakeUnitOfWork`, `FakeObjectStorage`, `FakeTextExtractor`, `FakePiiRedactor`, `FakeEvaluationQueue`,
`FakeLLMGateway`, `FakeWebhookClient`, `FixedClock`. Fakes record calls for assertions.

## Fixtures
- `tests/fixtures/notes/*.txt`: clinical notes (synthetic PII; no real data) per scenario:
  `meets.txt`, `does_not_meet.txt`, `unclear.txt`, `excluded_code.txt`, `no_codes.txt`.
- PDFs generated from notes by `scripts/make_fixtures.py`.
- `tests/fixtures/s3_events/minio_put.json`.

## Privacy assertion (mandatory, every layer touching text; [ADR-001](../00-overview.md#4-adrs))
Helper `assert_no_pii(s: str)` checks fixture PII strings (names, SSN, phone, MRN) absent.
Applied to: persisted claim, queue message, LLM request, webhook payload, log capture.

## Coverage target
Domain + application ≥ 95 %. Overall ≥ 85 %. Enforced in CI via `pytest --cov --cov-fail-under`.

## TDD
Each use case implemented test-first with fakes (superpowers TDD flow). Adapter tests written before adapter.
