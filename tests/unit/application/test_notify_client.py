"""UC-08. Two things are load-bearing: the payload never carries claim text
(ADR-001), and every attempt is recorded on the claim so an operator can see why a
delivery is stuck."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from tests.fakes import FakeWebhookClient, FixedClock
from tests.pii import assert_no_pii

from ecet.application.errors import WebhookPermanentError, WebhookTransientError
from ecet.application.notifications import ClientNotification
from ecet.application.use_cases.notify_client import (
    REJECTED,
    TENANT_INACTIVE,
    UNREACHABLE,
    NotifyClient,
    tenant_inactive,
)
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.errors import TenantNotFound
from ecet.domain.evaluation import Decision, Evaluation
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.policy import Icd10Code
from ecet.domain.tenant import Tenant

NOW = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)
KEY = "tenants/tenant-a/claims/note-1.pdf"
POLICY_ID = PolicyId(uuid4())


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": "dev-hmac-tenant-a",
        }
    )


def build_claim(status: ClaimStatus = ClaimStatus.EVALUATED) -> Claim:
    return Claim(
        id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source=SourceObject(bucket="claims", key=KEY, etag="etag-1", size=2048),
        status=status,
        redacted=RedactedText(
            text="Patient <PERSON> with M54.5 after nine weeks.", redactor="fake"
        ),
        policy_ids=[POLICY_ID],
        evaluation=Evaluation(
            decision=Decision.MEETS_NECESSITY,
            confidence=0.91,
            matched_policy_id=POLICY_ID,
            cited_codes=[Icd10Code(code="M54.5")],
            rationale="Conservative therapy documented for nine weeks.",
            evidence_found=["conservative therapy >= 6 weeks"],
            evidence_missing=[],
            model="fake-deterministic",
            prompt_version="v1",
        ),
        created_at=NOW,
        updated_at=NOW,
    )


def build_use_case(webhook: FakeWebhookClient) -> NotifyClient:
    return NotifyClient(webhook, FixedClock(NOW))


async def test_a_successful_delivery_sends_the_full_payload() -> None:
    webhook = FakeWebhookClient()
    claim = build_claim()

    payload = await build_use_case(webhook).execute(
        claim, build_tenant(), outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
    )

    assert len(webhook.deliveries) == 1
    tenant, delivered = webhook.deliveries[0]
    assert tenant.id == "tenant-a"
    assert delivered == payload
    assert payload.event == "claim.triaged"
    assert payload.claim_id == claim.id
    assert payload.source_key == KEY
    assert payload.outcome == "MEETS_NECESSITY"
    assert payload.decided_by == "auto"
    assert payload.confidence == 0.91
    assert payload.matched_policy_id == POLICY_ID
    assert payload.cited_codes == ["M54.5"]
    assert payload.decided_at == NOW


async def test_the_payload_carries_no_claim_text() -> None:
    webhook = FakeWebhookClient()
    claim = build_claim()

    await build_use_case(webhook).execute(
        claim, build_tenant(), outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
    )

    body = webhook.deliveries[0][1].model_dump_json()
    assert_no_pii(body)
    assert "redacted_text" not in body
    assert "Patient <PERSON>" not in body
    assert "dev-hmac-tenant-a" not in body


async def test_execute_does_not_touch_the_claim() -> None:
    """The claim's attempt bookkeeping is the caller's job via `Delivery.apply_to`;
    `execute` only builds and sends the payload."""
    claim = build_claim()
    before_attempts = claim.notification_attempts
    claim.last_notify_error = "500"

    await build_use_case(FakeWebhookClient()).execute(
        claim, build_tenant(), outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
    )

    assert claim.notification_attempts == before_attempts
    assert claim.last_notify_error == "500"


@pytest.mark.parametrize(
    "error", [WebhookPermanentError("400"), WebhookTransientError("3 attempts failed")]
)
async def test_a_failed_delivery_re_raises_without_touching_the_claim(error: Exception) -> None:
    webhook = FakeWebhookClient(error=error)
    claim = build_claim()
    before_attempts = claim.notification_attempts

    with pytest.raises(type(error)):
        await build_use_case(webhook).execute(
            claim,
            build_tenant(),
            outcome=Decision.MEETS_NECESSITY,
            confidence=0.91,
            decided_by="auto",
        )

    assert claim.notification_attempts == before_attempts
    assert claim.last_notify_error is None


async def test_a_human_decision_reports_decided_by_human() -> None:
    webhook = FakeWebhookClient()
    claim = build_claim()

    payload = await build_use_case(webhook).execute(
        claim, build_tenant(), outcome=Decision.DOES_NOT_MEET, confidence=1.0, decided_by="human"
    )

    assert payload.decided_by == "human"
    assert payload.outcome == "DOES_NOT_MEET"
    assert payload.confidence == 1.0


async def test_a_claim_with_no_evaluation_still_notifies() -> None:
    # UC-09c (Phase 5) resolves a claim that failed evaluation, so `evaluation` may be
    # None at delivery time. The payload degrades rather than crashing.
    webhook = FakeWebhookClient()
    claim = build_claim()
    claim.evaluation = None

    payload = await build_use_case(webhook).execute(
        claim, build_tenant(), outcome=Decision.DOES_NOT_MEET, confidence=1.0, decided_by="human"
    )

    assert payload.matched_policy_id is None
    assert payload.cited_codes == []
    assert payload.rationale == ""
    assert isinstance(payload, ClientNotification)


async def test_attempt_reports_what_to_do_without_touching_the_claim() -> None:
    """The caller saves a claim it re-read after the POST; a NotifyClient that mutated
    the claim it was handed would write those fields onto a stale object."""
    claim = build_claim(status=ClaimStatus.EVALUATED)
    before = claim.notification_attempts

    delivery = await NotifyClient(FakeWebhookClient(), FixedClock(NOW)).attempt(
        claim, build_tenant(), outcome=Decision.MEETS_NECESSITY, confidence=0.9, decided_by="auto"
    )

    assert delivery.succeeded
    assert delivery.failure_reason is None
    assert claim.notification_attempts == before  # untouched

    delivery.apply_to(claim)
    assert claim.notification_attempts == before + 1
    assert claim.last_notify_error is None


@pytest.mark.parametrize(
    ("error", "token"),
    [
        (WebhookPermanentError("400"), REJECTED),
        (WebhookTransientError("3 attempts failed"), UNREACHABLE),
    ],
)
async def test_attempt_folds_a_delivery_failure_into_its_token(
    error: Exception, token: str
) -> None:
    claim = build_claim()

    delivery = await build_use_case(FakeWebhookClient(error=error)).attempt(
        claim, build_tenant(), outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
    )
    delivery.apply_to(claim)

    assert delivery.failure_reason == token
    assert claim.notification_attempts == 1
    assert claim.last_notify_error is not None
    assert type(error).__name__ in claim.last_notify_error


async def test_a_rejected_delivery_carries_the_token_and_the_detail() -> None:
    claim = build_claim(status=ClaimStatus.EVALUATED)

    delivery = await NotifyClient(
        FakeWebhookClient(error=WebhookPermanentError("status 400")), FixedClock(NOW)
    ).attempt(
        claim, build_tenant(), outcome=Decision.MEETS_NECESSITY, confidence=0.9, decided_by="auto"
    )
    delivery.apply_to(claim)

    assert delivery.failure_reason == REJECTED
    assert claim.last_notify_error is not None
    assert claim.last_notify_error.startswith("WebhookPermanentError")
    assert claim.notification_attempts == 1


def test_a_deactivated_tenant_is_a_delivery_that_never_happened() -> None:
    delivery = tenant_inactive(TenantNotFound("tenant-a"))

    assert delivery.failure_reason == TENANT_INACTIVE
    assert delivery.error is not None
    assert not delivery.succeeded


def test_a_deactivated_tenant_does_not_count_as_an_attempt() -> None:
    """`notification_attempts` counts the times the system tried to tell the client;
    a tenant deactivated before delivery means nothing left the process."""
    claim = build_claim()
    before = claim.notification_attempts

    tenant_inactive(TenantNotFound("tenant-a")).apply_to(claim)

    assert claim.notification_attempts == before
    assert claim.last_notify_error is not None


async def test_attempt_returns_a_successful_delivery_when_the_webhook_is_delivered() -> None:
    webhook = FakeWebhookClient()

    delivery = await build_use_case(webhook).attempt(
        build_claim(),
        build_tenant(),
        outcome=Decision.MEETS_NECESSITY,
        confidence=0.91,
        decided_by="auto",
    )

    assert delivery.succeeded
    assert len(webhook.deliveries) == 1
