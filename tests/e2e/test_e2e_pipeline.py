"""The demo, as assertions. Each test drops its own object under a unique key, so the
tests are independent of each other and of whatever an earlier `make demo` left."""

import json

from tests.e2e.stack import (
    api,
    claim_row,
    compose_logs,
    deliveries_for,
    delivery_for,
    drop,
    eventually,
    log_records,
    worker_metrics,
)
from tests.pii import assert_no_pii


async def _claim_in(claim_id: str, tenant: str, status: str) -> dict[str, object] | None:
    body: dict[str, object] = (
        (await api("GET", f"/v1/claims/{claim_id}", tenant=tenant)).raise_for_status().json()
    )
    return body if body["status"] == status else None


async def test_a_note_that_meets_policy_is_approved_and_delivered_signed() -> None:
    key = await drop("note_simple", "tenant-a")

    delivery = await eventually(
        lambda: delivery_for(key, decided_by="auto"), what=f"the auto webhook for {key}"
    )
    payload = delivery["payload"]
    assert delivery["hook"] == "northwind"
    assert delivery["verified"] is True
    assert payload["outcome"] == "MEETS_NECESSITY"
    assert payload["confidence"] >= 0.85

    # The webhook is sent before the write that records it commits (Phase 6), so the
    # status is polled rather than read once.
    claim = await eventually(
        lambda: _claim_in(payload["claim_id"], "tenant-a", "APPROVED_AUTO"),
        what="the claim to reach APPROVED_AUTO",
    )
    assert_no_pii(json.dumps(delivery))
    assert_no_pii(json.dumps(claim))


async def test_an_unclear_note_waits_for_a_human_whose_decision_is_delivered() -> None:
    key = await drop("note_unclear", "tenant-a")

    row = await eventually(
        lambda: claim_row(key, status="REVIEW_PENDING"), what="the claim to reach REVIEW_PENDING"
    )
    claim_id = str(row["id"])
    assert await deliveries_for(key) == [], "nobody has decided anything yet"

    async def open_task() -> dict[str, object] | None:
        tasks = (await api("GET", "/v1/reviews?limit=200", tenant="tenant-a")).raise_for_status()
        return next((t for t in tasks.json() if t["claim_id"] == claim_id), None)

    task = await eventually(open_task, what="the review task")
    assert task["redacted_text"]
    assert_no_pii(json.dumps(task))

    resolved = await api(
        "POST",
        f"/v1/reviews/{task['task_id']}/resolve",
        tenant="tenant-a",
        json={
            "reviewer": "e2e.reviewer",
            "resolution": "MEETS_NECESSITY",
            "notes": "Therapy dates confirmed with the provider.",
        },
    )
    assert resolved.status_code == 200
    assert resolved.json()["claim_status"] == "REVIEW_RESOLVED"

    delivery = await eventually(
        lambda: delivery_for(key, decided_by="human"), what="the human webhook"
    )
    assert delivery["verified"] is True
    assert delivery["payload"]["outcome"] == "MEETS_NECESSITY"
    assert delivery["payload"]["claim_id"] == claim_id
    assert_no_pii(json.dumps(delivery))


async def test_an_excluded_code_is_rejected_without_calling_the_llm() -> None:
    """ADR-002: the deterministic checks reject it at ingestion, a human gets the task,
    and the worker never sees the claim."""
    key = await drop("note_excluded_code", "tenant-a")

    row = await eventually(
        lambda: claim_row(key, status="REVIEW_PENDING"), what="the deterministic reject"
    )
    claim_id = str(row["id"])

    tasks = (await api("GET", "/v1/reviews?limit=200", tenant="tenant-a")).raise_for_status()
    (task,) = [t for t in tasks.json() if t["claim_id"] == claim_id]
    assert task["reason"] == "DETERMINISTIC_REJECT"
    assert task["evaluation"] is None
    assert not [r for r in log_records("worker") if r.get("claim_id") == claim_id]
    assert_no_pii(json.dumps(task))


async def test_a_tenant_without_policies_records_no_policies_and_notifies_no_one() -> None:
    """ADR-005: no policy context, no evaluation."""
    key = await drop("note_simple", "tenant-empty")

    row = await eventually(
        lambda: claim_row(key, status="NO_POLICIES"), what="the claim to reach NO_POLICIES"
    )
    assert row["failure_reason"] == "no_policies"
    assert await deliveries_for(key) == []
    assert_no_pii(json.dumps(row, default=str))


async def test_one_request_id_follows_a_claim_from_the_api_into_the_worker() -> None:
    """Phase 6's done-when, end to end: grep one id across both processes."""
    key = await drop("note_simple", "tenant-a")
    delivery = await eventually(
        lambda: delivery_for(key, decided_by="auto"), what=f"the auto webhook for {key}"
    )
    claim_id = delivery["payload"]["claim_id"]

    def request_ids(service: str) -> set[str]:
        return {
            str(r["request_id"])
            for r in log_records(service)
            if r.get("claim_id") == claim_id and "request_id" in r
        }

    assert request_ids("api") & request_ids("worker")
    assert_no_pii(json.dumps(delivery))


async def test_both_processes_expose_the_pipeline_metrics() -> None:
    key = await drop("note_simple", "tenant-a")
    await eventually(lambda: delivery_for(key, decided_by="auto"), what="the auto webhook")

    api_metrics = (await api("GET", "/metrics")).raise_for_status().text
    assert 'ecet_ingest_seconds_count{outcome="ingested"}' in api_metrics
    assert "ecet_deterministic_verdict_total" in api_metrics

    worker = worker_metrics()
    assert 'ecet_triage_route_total{route="AUTO_NOTIFY"}' in worker
    assert "ecet_llm_calls_total" in worker
    assert 'ecet_webhook_attempts_total{status_class="2xx"}' in worker


async def test_no_service_log_carries_pii() -> None:
    """The testing spec's log-capture privacy assertion, on the real sinks. Every note
    fixture carries the full synthetic PII block, and this runs after a drop."""
    key = await drop("note_simple", "tenant-a")
    await eventually(lambda: delivery_for(key, decided_by="auto"), what="the auto webhook")

    for service in ("api", "worker", "mock-client"):
        assert_no_pii(compose_logs(service))
