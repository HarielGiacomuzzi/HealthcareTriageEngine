# Platform: Configuration

`ecet/config.py` — `class Settings(BaseSettings)`, env prefix `ECET_`, `.env` file support.
Consumed by [api](../04-interfaces/api.md) and [worker](../04-interfaces/worker.md); wired in [docker-compose](docker-compose.md).
All secrets `SecretStr`. Settings built once in CLI, passed into containers; never imported as global.

| Env var                      | Type      | Default                          | Used by |
|------------------------------|-----------|----------------------------------|---------|
| ECET_ENV                     | str       | `dev`                            | all     |
| ECET_LOG_LEVEL               | str       | `INFO`                           | all     |
| ECET_DATABASE_URL            | SecretStr | `postgresql+asyncpg://ecet:ecet@postgres:5432/ecet` | all |
| ECET_AUTO_MIGRATE            | bool      | `false`                          | api     |
| ECET_AMQP_URL                | SecretStr | `amqp://guest:guest@rabbitmq:5672/` | all  |
| ECET_WORKER_PREFETCH         | int       | `4`                              | worker  |
| ECET_S3_ENDPOINT             | str\|None | `http://minio:9000`              | api     |
| ECET_S3_ACCESS_KEY / SECRET_KEY | SecretStr | minio creds                   | api     |
| ECET_S3_EVENT_TOKEN          | SecretStr | required                         | api     |
| ECET_API_KEY                 | SecretStr | required                         | api     |
| ECET_MAX_PDF_BYTES           | int       | `20_000_000`                     | api     |
| ECET_MAX_PDF_PAGES           | int       | `50`                             | api     |
| ECET_PII_CONCURRENCY         | int       | `2`                              | api     |
| ECET_SPACY_MODEL             | str       | `en_core_web_lg`                 | api     |
| ECET_CONFIDENCE_THRESHOLD    | float     | `0.85`  (0 < x ≤ 1)              | worker  |
| ECET_LLM_PROVIDER            | enum      | `fake` \| `openai`               | worker  |
| ECET_LLM_BASE_URL            | str       | `https://api.anthropic.com/v1/`  | worker  |
| ECET_LLM_API_KEY             | SecretStr | none (required if provider=openai) | worker |
| ECET_LLM_MODEL               | str       | `claude-sonnet-5`                | worker  |
| ECET_LLM_TIMEOUT_S           | int       | `60`                             | worker  |
| ECET_PROMPT_VERSION          | str       | `v1`                             | worker  |
| ECET_WEBHOOK_TIMEOUT_S       | int       | `10`                             | worker/api |
| ECET_WEBHOOK_MAX_ATTEMPTS    | int       | `3`                              | worker/api |
| ECET_METRICS_PORT            | int       | `9100`                           | worker  |

`.env.example` lists all with dev values. Validation errors at startup → exit 1 with field list.
