"""Every `ecet_*` metric, defined once.

Top-level, beside `config.py`, rather than inside a layer: the ingestion metrics
belong to use cases, the vendor metrics to adapters and the gauges to the worker, so
no layer owns them. There is nothing to inject either — `prometheus_client`'s default
registry is process-global, and a metric object is a handle on it.

`ecet.domain` must not import this module; `make imports` enforces it. The domain is
pure functions over pure models, and a counter is I/O with extra steps.

Counter names keep the spec's `_total` suffix: `prometheus_client` strips it from the
metric name and appends it back on exposition, so the scraped name is the spec's.
"""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

INGEST_SECONDS = Histogram(
    "ecet_ingest_seconds",
    "UC-01 wall time, from the command to the stored claim.",
    ["outcome"],
)
PDF_EXTRACT_SECONDS = Histogram("ecet_pdf_extract_seconds", "PDF text extraction wall time.")
PII_REDACTION_SECONDS = Histogram("ecet_pii_redaction_seconds", "UC-02 redaction wall time.")
PII_ENTITIES_TOTAL = Counter(
    "ecet_pii_entities_total", "Entities redacted before any egress (ADR-001).", ["entity"]
)
DETERMINISTIC_VERDICT_TOTAL = Counter(
    "ecet_deterministic_verdict_total", "UC-04 verdicts (ADR-002).", ["verdict"]
)
LLM_CALLS_AVOIDED_TOTAL = Counter(
    "ecet_llm_calls_avoided_total",
    "Claims a deterministic REJECT sent straight to a human, so the vendor was never called.",
)
LLM_CALLS_TOTAL = Counter(
    "ecet_llm_calls_total", "Vendor evaluations attempted.", ["provider", "model", "outcome"]
)
LLM_LATENCY_SECONDS = Histogram(
    "ecet_llm_latency_seconds", "Vendor round-trip time.", ["provider", "model"]
)
LLM_TOKENS_TOTAL = Counter("ecet_llm_tokens_total", "Tokens billed by the vendor.", ["direction"])
TRIAGE_ROUTE_TOTAL = Counter("ecet_triage_route_total", "UC-07 routes taken (ADR-003).", ["route"])
WEBHOOK_ATTEMPTS_TOTAL = Counter(
    "ecet_webhook_attempts_total", "Webhook POSTs, by response class.", ["status_class"]
)
CLAIMS_BY_STATUS = Gauge(
    "ecet_claims_by_status", "Claims per status; the worker refreshes it.", ["status"]
)
REVIEW_OPEN = Gauge("ecet_review_open", "Open review tasks per tenant.", ["tenant"])
DB_POOL_IN_USE = Gauge(
    "ecet_db_pool_in_use",
    "Checked-out SQLAlchemy connections. A connection held across an LLM call or a "
    "webhook POST shows up here first.",
)


def start_metrics_server(port: int) -> None:
    """Serve `/metrics` on `port` from a daemon thread (the worker; the api mounts the
    ASGI app instead). `prometheus_client` owns the thread and the socket."""
    start_http_server(port)
