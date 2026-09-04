# Platform: Observability

## Logging
- `structlog` JSON to stdout. Bound context per request/message: `request_id`/`message_id`, `claim_id`, `tenant_id`.
- Redaction guard processor: drops any log field named `text`, `raw_text`, `redacted_text`, `webhook_secret`, `api_key`. Belt-and-braces for [ADR-001](../00-overview.md#4-adrs).
- One `event="claim.transition"` log per state change with `from`, `to`, `reason`.

## Metrics (prometheus_client, `/metrics` on api :8000 and worker :9100)
| Metric                                   | Type      | Labels |
|------------------------------------------|-----------|--------|
| `ecet_ingest_seconds`                    | histogram | outcome |
| `ecet_pdf_extract_seconds`               | histogram | |
| `ecet_pii_redaction_seconds`             | histogram | |
| `ecet_pii_entities_total`                | counter   | entity |
| `ecet_deterministic_verdict_total`       | counter   | verdict |
| `ecet_llm_calls_avoided_total`           | counter   | |
| `ecet_llm_calls_total`                   | counter   | provider, model, outcome |
| `ecet_llm_latency_seconds`               | histogram | provider, model |
| `ecet_llm_tokens_total`                  | counter   | direction |
| `ecet_triage_route_total`                | counter   | route |
| `ecet_webhook_attempts_total`            | counter   | status_class |
| `ecet_claims_by_status`                  | gauge     | status (refreshed by worker every 30 s) |
| `ecet_review_open`                       | gauge     | tenant |

## Tracing
Out of scope v1. `request_id` propagated api → message header `x-request-id` → worker logs. Enough to grep one claim end-to-end.

## Optional compose profile `observability`
Prometheus + Grafana with one dashboard JSON (ingest latency, LLM avoided %, route split). [Roadmap phase 6](../06-roadmap.md#phase-6--observability--polish); nice portfolio visual.
