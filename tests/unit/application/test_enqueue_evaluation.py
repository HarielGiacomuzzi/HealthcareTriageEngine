from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

import pytest
from tests.fakes import FakeEvaluationQueue, FixedClock
from tests.pii import assert_no_pii

from ecet.application.errors import QueuePublishError
from ecet.application.use_cases.enqueue_evaluation import EnqueueEvaluation
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.evaluation import CheckOutcome, DeterministicResult, Verdict
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.policy import Policy

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


def build_policy() -> Policy:
    return Policy.model_validate(
        {
            "id": PolicyId(uuid4()),
            "tenant_id": "tenant-a",
            "name": "MRI lumbar spine",
            "version": 2,
            "covered_codes": [{"code": "M54.5"}],
            "criteria_text": "Imaging is covered after six weeks of conservative therapy.",
            "effective_from": date(2026, 1, 1),
        }
    )


def build_claim(**overrides: Any) -> Claim:
    fields: dict[str, Any] = {
        "id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "source": SourceObject(
            bucket="claims",
            key="tenants/tenant-a/claims/note.pdf",
            etag="etag-1",
            size=12_345,
        ),
        "status": ClaimStatus.POLICIES_ATTACHED,
        "redacted": RedactedText(
            text="Patient <PERSON> with chronic low back pain, M54.5, for eleven months.",
            entity_counts={"PERSON": 1},
            redactor="fake",
        ),
        "deterministic": DeterministicResult(
            verdict=Verdict.PASS,
            checks=[CheckOutcome(name="icd10_present", passed=True)],
        ),
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return Claim.model_validate(fields)


async def test_it_publishes_one_message_built_from_the_claim() -> None:
    queue = FakeEvaluationQueue()
    claim = build_claim()

    message_id = await EnqueueEvaluation(queue, FixedClock(NOW)).execute(claim, [build_policy()])

    (published,) = queue.published
    assert published.message_id == message_id
    assert published.claim_id == claim.id
    assert published.tenant_id == "tenant-a"
    assert published.deterministic_verdict == "PASS"
    assert published.found_codes == ["M54.5"]
    assert published.enqueued_at == NOW
    assert [snapshot.name for snapshot in published.policies] == ["MRI lumbar spine"]
    assert_no_pii(published.model_dump_json())


async def test_a_publish_failure_propagates_so_the_claim_stays_policies_attached() -> None:
    queue = FakeEvaluationQueue(error=QueuePublishError("no confirm"))

    with pytest.raises(QueuePublishError):
        await EnqueueEvaluation(queue, FixedClock(NOW)).execute(build_claim(), [build_policy()])

    assert queue.published == []


async def test_a_claim_without_redacted_text_is_a_programming_error() -> None:
    claim = build_claim(redacted=None)

    with pytest.raises(ValueError, match="redacted"):
        await EnqueueEvaluation(FakeEvaluationQueue(), FixedClock(NOW)).execute(
            claim, [build_policy()]
        )


async def test_a_rejected_claim_never_reaches_the_queue() -> None:
    claim = build_claim(
        deterministic=DeterministicResult(
            verdict=Verdict.REJECT,
            checks=[CheckOutcome(name="excluded_code_hit", passed=False)],
        )
    )

    with pytest.raises(ValueError, match="REJECT"):
        await EnqueueEvaluation(FakeEvaluationQueue(), FixedClock(NOW)).execute(
            claim, [build_policy()]
        )
