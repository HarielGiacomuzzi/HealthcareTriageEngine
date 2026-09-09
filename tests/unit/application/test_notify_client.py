"""UC-08. Two things are load-bearing: the payload never carries claim text
(ADR-001), and every attempt is recorded on the claim so an operator can see why a
delivery is stuck."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from tests.fakes import FakeTenantRepository, FakeWebhookClient, FixedClock
from tests.pii import assert_no_pii

from ecet.application.errors import WebhookPermanentError, WebhookTransientError
from ecet.application.notifications import ClientNotification
from ecet.application.use_cases.notify_client import NotifyClient
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
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


def build_claim() -> Claim:
    return Claim(
        id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source=SourceObject(bucket="claims", key=KEY, etag="etag-1", size=2048),
        status=ClaimStatus.EVALUATED,
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
    return NotifyClient(FakeTenantRepository([build_tenant()]), webhook, FixedClock(NOW))


async def test_a_successful_delivery_sends_the_full_payload() -> None:
    webhook = FakeWebhookClient()
    claim = build_claim()

    payload = await build_use_case(webhook).execute(
        claim, outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
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
        claim, outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
    )

    body = webhook.deliveries[0][1].model_dump_json()
    assert_no_pii(body)
    assert "redacted_text" not in body
    assert "Patient <PERSON>" not in body
    assert "dev-hmac-tenant-a" not in body


async def test_a_successful_delivery_records_the_attempt_and_clears_the_error() -> None:
    claim = build_claim()
    claim.notification_attempts = 2
    claim.last_notify_error = "500"

    await build_use_case(FakeWebhookClient()).execute(
        claim, outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
    )

    assert claim.notification_attempts == 3
    assert claim.last_notify_error is None


@pytest.mark.parametrize(
    "error", [WebhookPermanentError("400"), WebhookTransientError("3 attempts failed")]
)
async def test_a_failed_delivery_records_the_error_and_re_raises(error: Exception) -> None:
    webhook = FakeWebhookClient(error=error)
    claim = build_claim()

    with pytest.raises(type(error)):
        await build_use_case(webhook).execute(
            claim, outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
        )

    assert claim.notification_attempts == 1
    assert claim.last_notify_error is not None
    assert type(error).__name__ in claim.last_notify_error


async def test_a_human_decision_reports_decided_by_human() -> None:
    webhook = FakeWebhookClient()
    claim = build_claim()

    payload = await build_use_case(webhook).execute(
        claim, outcome=Decision.DOES_NOT_MEET, confidence=1.0, decided_by="human"
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
        claim, outcome=Decision.DOES_NOT_MEET, confidence=1.0, decided_by="human"
    )

    assert payload.matched_policy_id is None
    assert payload.cited_codes == []
    assert payload.rationale == ""
    assert isinstance(payload, ClientNotification)
