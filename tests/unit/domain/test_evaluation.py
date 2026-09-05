from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from ecet.domain.errors import ReviewAlreadyResolved
from ecet.domain.evaluation import (
    CheckOutcome,
    Decision,
    DeterministicResult,
    Evaluation,
    ReviewReason,
    ReviewStatus,
    ReviewTask,
    Route,
    Verdict,
    triage,
)
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.policy import Icd10Code

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)


def build_evaluation(**overrides: Any) -> Evaluation:
    fields: dict[str, Any] = {
        "decision": Decision.MEETS_NECESSITY,
        "confidence": 0.9,
        "matched_policy_id": PolicyId(uuid4()),
        "cited_codes": [Icd10Code(code="M54.5")],
        "rationale": "Conservative therapy documented for eight weeks.",
        "evidence_found": ["imaging report"],
        "evidence_missing": [],
        "model": "claude-sonnet-5",
        "prompt_version": "v1",
        "latency_ms": 1200,
        "input_tokens": 900,
        "output_tokens": 120,
    }
    fields.update(overrides)
    return Evaluation.model_validate(fields)


def build_task(**overrides: Any) -> ReviewTask:
    fields: dict[str, Any] = {
        "id": uuid4(),
        "claim_id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "reason": ReviewReason.LOW_CONFIDENCE,
        "created_at": NOW,
    }
    fields.update(overrides)
    return ReviewTask.model_validate(fields)


@pytest.mark.parametrize("confidence", [-0.01, 1.01])
def test_confidence_outside_zero_to_one_is_rejected(confidence: float) -> None:
    with pytest.raises(ValidationError):
        build_evaluation(confidence=confidence)


def test_rationale_is_capped_at_two_thousand_characters() -> None:
    with pytest.raises(ValidationError):
        build_evaluation(rationale="x" * 2001)


def test_evaluation_is_frozen() -> None:
    evaluation = build_evaluation()
    with pytest.raises(ValidationError):
        evaluation.confidence = 0.1  # type: ignore[misc]


@pytest.mark.parametrize(
    ("decision", "confidence", "expected"),
    [
        (Decision.MEETS_NECESSITY, 0.85, Route.AUTO_NOTIFY),
        (Decision.MEETS_NECESSITY, 0.8499, Route.HUMAN_REVIEW),
        (Decision.DOES_NOT_MEET, 0.99, Route.AUTO_NOTIFY),
        (Decision.INSUFFICIENT_EVIDENCE, 0.99, Route.HUMAN_REVIEW),
        (Decision.MEETS_NECESSITY, 0.0, Route.HUMAN_REVIEW),
    ],
)
def test_triage_boundaries(decision: Decision, confidence: float, expected: Route) -> None:
    evaluation = build_evaluation(decision=decision, confidence=confidence)
    assert triage(evaluation, threshold=0.85) is expected


def test_triage_uses_the_injected_threshold() -> None:
    evaluation = build_evaluation(confidence=0.7)
    assert triage(evaluation, threshold=0.6) is Route.AUTO_NOTIFY
    assert triage(evaluation, threshold=0.8) is Route.HUMAN_REVIEW


def test_deterministic_result_round_trips_as_json() -> None:
    result = DeterministicResult(
        verdict=Verdict.UNCERTAIN,
        checks=[CheckOutcome(name="icd10_present", passed=False, detail="no codes found")],
    )
    assert DeterministicResult.model_validate(result.model_dump(mode="json")) == result


def test_review_task_starts_open_and_unresolved() -> None:
    task = build_task()
    assert task.status is ReviewStatus.OPEN
    assert task.resolution is None
    assert task.resolved_at is None


def test_resolving_a_task_records_the_decision() -> None:
    task = build_task()
    task.resolve(
        resolution=Decision.DOES_NOT_MEET,
        reviewer="nurse@tenant-a.example",
        notes="No conservative therapy documented.",
        now=NOW,
    )
    assert task.status is ReviewStatus.RESOLVED
    assert task.resolution is Decision.DOES_NOT_MEET
    assert task.reviewer == "nurse@tenant-a.example"
    assert task.resolved_at == NOW


def test_resolving_twice_raises() -> None:
    task = build_task()
    task.resolve(resolution=Decision.MEETS_NECESSITY, reviewer="a", notes=None, now=NOW)
    with pytest.raises(ReviewAlreadyResolved):
        task.resolve(resolution=Decision.DOES_NOT_MEET, reviewer="b", notes=None, now=NOW)


def test_a_human_may_not_resolve_as_insufficient_evidence() -> None:
    task = build_task()
    with pytest.raises(ValueError, match="MEETS_NECESSITY"):
        task.resolve(
            resolution=Decision.INSUFFICIENT_EVIDENCE,
            reviewer="a",
            notes=None,
            now=NOW,
        )
    assert task.status is ReviewStatus.OPEN
