"""The metric names are a contract with whoever scrapes them, so they get pinned.

`prometheus_client` strips a trailing `_total` from a Counter's name and appends it
again on exposition; these assertions are what stops someone "fixing" that.
"""

from prometheus_client import REGISTRY, generate_latest

from ecet import metrics

SPEC_NAMES = (
    "ecet_ingest_seconds",
    "ecet_pdf_extract_seconds",
    "ecet_pii_redaction_seconds",
    "ecet_pii_entities_total",
    "ecet_deterministic_verdict_total",
    "ecet_llm_calls_avoided_total",
    "ecet_llm_calls_total",
    "ecet_llm_latency_seconds",
    "ecet_llm_tokens_total",
    "ecet_triage_route_total",
    "ecet_webhook_attempts_total",
    "ecet_claims_by_status",
    "ecet_review_open",
)


def test_every_metric_the_spec_lists_is_exposed_under_its_own_name() -> None:
    metrics.INGEST_SECONDS.labels(outcome="ingested").observe(0.01)
    metrics.PDF_EXTRACT_SECONDS.observe(0.01)
    metrics.PII_REDACTION_SECONDS.observe(0.01)
    metrics.PII_ENTITIES_TOTAL.labels(entity="PERSON").inc()
    metrics.DETERMINISTIC_VERDICT_TOTAL.labels(verdict="PASS").inc()
    metrics.LLM_CALLS_AVOIDED_TOTAL.inc()
    metrics.LLM_CALLS_TOTAL.labels(provider="fake", model="m", outcome="ok").inc()
    metrics.LLM_LATENCY_SECONDS.labels(provider="fake", model="m").observe(0.01)
    metrics.LLM_TOKENS_TOTAL.labels(direction="input").inc(3)
    metrics.TRIAGE_ROUTE_TOTAL.labels(route="AUTO_NOTIFY").inc()
    metrics.WEBHOOK_ATTEMPTS_TOTAL.labels(status_class="2xx").inc()
    metrics.CLAIMS_BY_STATUS.labels(status="QUEUED").set(2)
    metrics.REVIEW_OPEN.labels(tenant="tenant-a").set(1)

    exposition = generate_latest().decode("utf-8")

    for name in SPEC_NAMES:
        assert f"{name} " in exposition or f"{name}{{" in exposition, name


def test_the_pool_gauge_reads_a_callable() -> None:
    """`DB_POOL_IN_USE` is fed by `engine.pool.checkedout`, which is a function, not a
    number a caller remembers to set."""
    metrics.DB_POOL_IN_USE.set_function(lambda: 7.0)

    assert REGISTRY.get_sample_value("ecet_db_pool_in_use") == 7.0


def test_no_metric_carries_an_unbounded_label() -> None:
    """ADR-001 and cardinality: a claim id, a tenant's URL or a key must never become
    a label. `tenant` on `ecet_review_open` is the one identifier the spec allows."""
    forbidden = {"claim_id", "key", "url", "api_key", "text", "reviewer"}

    for collector in (
        metrics.INGEST_SECONDS,
        metrics.PII_ENTITIES_TOTAL,
        metrics.LLM_CALLS_TOTAL,
        metrics.TRIAGE_ROUTE_TOTAL,
        metrics.WEBHOOK_ATTEMPTS_TOTAL,
        metrics.CLAIMS_BY_STATUS,
        metrics.REVIEW_OPEN,
    ):
        assert not forbidden & set(collector._labelnames)
