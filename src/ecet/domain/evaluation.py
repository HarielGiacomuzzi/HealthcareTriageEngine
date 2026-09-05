"""Evaluation results and the triage decision.

Three separate things live here because they are the same subject at three stages:
what the cheap rules concluded (`DeterministicResult`), what the LLM concluded
(`Evaluation`), and what a human is asked to conclude (`ReviewTask`).
"""

from datetime import datetime
from enum import StrEnum
from typing import Literal, get_args
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from ecet.domain.errors import InvalidReviewResolution, ReviewAlreadyResolved
from ecet.domain.ids import ClaimId, PolicyId, TenantIdField
from ecet.domain.policy import Icd10Code


class Verdict(StrEnum):
    """Outcome of the deterministic pre-LLM checks (ADR-002)."""

    PASS = "PASS"
    REJECT = "REJECT"
    UNCERTAIN = "UNCERTAIN"


class CheckOutcome(BaseModel):
    """One deterministic rule's result."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    passed: bool
    detail: str = ""


class DeterministicResult(BaseModel):
    """Aggregated deterministic checks. Persisted as jsonb on the claim."""

    model_config = ConfigDict(frozen=True)

    verdict: Verdict
    checks: list[CheckOutcome] = Field(default_factory=list)


class Decision(StrEnum):
    """Medical-necessity decision, vendor-neutral."""

    MEETS_NECESSITY = "MEETS_NECESSITY"
    DOES_NOT_MEET = "DOES_NOT_MEET"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class Evaluation(BaseModel):
    """Validated LLM output. Vendor JSON is parsed straight into this model."""

    model_config = ConfigDict(frozen=True)

    decision: Decision
    confidence: float = Field(ge=0.0, le=1.0)
    matched_policy_id: PolicyId | None = None
    cited_codes: list[Icd10Code] = Field(default_factory=list)
    rationale: str = Field(default="", max_length=2000)
    evidence_found: list[str] = Field(default_factory=list)
    evidence_missing: list[str] = Field(default_factory=list)
    model: str = ""
    prompt_version: str = ""
    latency_ms: int = Field(default=0, ge=0)
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)


class Route(StrEnum):
    AUTO_NOTIFY = "AUTO_NOTIFY"
    HUMAN_REVIEW = "HUMAN_REVIEW"


def triage(evaluation: Evaluation, threshold: float) -> Route:
    """ADR-003: only a confident, decisive evaluation may skip a human.

    `threshold` is injected by the caller (`ECET_CONFIDENCE_THRESHOLD`); the domain
    never reads configuration.
    """
    if evaluation.decision is Decision.INSUFFICIENT_EVIDENCE:
        return Route.HUMAN_REVIEW
    return Route.AUTO_NOTIFY if evaluation.confidence >= threshold else Route.HUMAN_REVIEW


class ReviewReason(StrEnum):
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    DETERMINISTIC_REJECT = "DETERMINISTIC_REJECT"
    DETERMINISTIC_UNCERTAIN_LLM_LOW = "DETERMINISTIC_UNCERTAIN_LLM_LOW"
    EVALUATION_FAILED = "EVALUATION_FAILED"


class ReviewStatus(StrEnum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


HumanResolution = Literal[Decision.MEETS_NECESSITY, Decision.DOES_NOT_MEET]

#: The same set at runtime — the Literal is only a static guarantee.
_HUMAN_RESOLUTIONS: frozenset[Decision] = frozenset(get_args(HumanResolution))


class ReviewTask(BaseModel):
    """Human-in-the-loop work item. One open task per claim."""

    model_config = ConfigDict(validate_assignment=True)

    id: UUID
    claim_id: ClaimId
    tenant_id: TenantIdField
    reason: ReviewReason
    status: ReviewStatus = ReviewStatus.OPEN
    resolution: HumanResolution | None = None
    reviewer: str | None = None
    notes: str | None = None
    created_at: AwareDatetime
    resolved_at: AwareDatetime | None = None

    def resolve(
        self,
        *,
        resolution: HumanResolution,
        reviewer: str,
        notes: str | None,
        now: datetime,
    ) -> None:
        """Record a human decision. `now` is injected — the domain owns no clock."""
        if self.status is ReviewStatus.RESOLVED:
            raise ReviewAlreadyResolved(str(self.id))
        if resolution not in _HUMAN_RESOLUTIONS:
            raise InvalidReviewResolution("resolution must be MEETS_NECESSITY or DOES_NOT_MEET")
        self.resolution = resolution
        self.reviewer = reviewer
        self.notes = notes
        self.resolved_at = now
        self.status = ReviewStatus.RESOLVED
