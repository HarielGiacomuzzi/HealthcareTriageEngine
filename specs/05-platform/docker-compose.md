# Platform: Docker & Compose

## Dockerfile (root, single image)
Multi-stage:
1. `builder`: `python:3.12-slim`, install `uv`, `uv sync --frozen --no-dev`, `spacy download en_core_web_lg` into venv.
2. `runtime`: `python:3.12-slim`, copy venv + `src/`, non-root user, `ENTRYPOINT ["ecet"]`, `CMD ["api"]`.
Healthcheck: `curl -f localhost:8000/readyz` (api). Worker overrides cmd + healthcheck in compose.
Image target ≈ 1.5 GB (spaCy lg). Document; `en_core_web_md` swap via build arg `SPACY_MODEL` for smaller demo.

## docker-compose.yml services

| Service       | Image / build         | Ports        | Depends on (healthy) |
|---------------|-----------------------|--------------|----------------------|
| postgres      | postgres:16           | 5432         | —                    |
| rabbitmq      | rabbitmq:3.13-management | 5672, 15672 | —                 |
| minio         | minio/minio           | 9000, 9001   | —                    |
| minio-setup   | minio/mc (one-shot)   | —            | minio, api           |
| api           | build .  cmd `api`    | 8000         | postgres, rabbitmq   |
| worker        | build .  cmd `worker` | 9100         | postgres, rabbitmq   |
| mock-client   | build services/mock-client | 8081    | —                    |

Notes:
- `minio-setup` depends on `api` healthy because webhook target registration is validated by MinIO at `mc event add`.
- `api` runs `alembic upgrade head` + seed on start when `ECET_AUTO_MIGRATE=true` (compose only).
- Named volumes: `pgdata`, `miniodata`, `rabbitdata`.
- All services on one network; `.env` supplies secrets (see [config spec](config.md)).
- Default `ECET_LLM_PROVIDER=fake` → zero external calls; `docker compose up --build` works offline after image pull.

## Demo flow (`make demo`)
```
docker compose up --build -d
./scripts/demo_drop.sh tests/fixtures/pdfs/note_simple.pdf tenant-a
# → api log: ingested claim … QUEUED
# → worker log: evaluated … confidence 0.91 → APPROVED_AUTO
curl localhost:8081/received | jq
```
Second drop with [`tenant-empty`](../01-domain/tenant.md#4-seed) → 422 NO_POLICIES shown in api log.
Third drop with note containing "unclear" → REVIEW_PENDING; resolve via [`POST /v1/reviews/{id}/resolve`](../04-interfaces/api.md#endpoints) ([UC-09c](../02-use-cases/UC-09-human-review.md#uc-09c-resolvereview)).

## Resource limits (compose `deploy.resources`)
api 2 GB RAM (presidio), worker 512 MB, others default.
