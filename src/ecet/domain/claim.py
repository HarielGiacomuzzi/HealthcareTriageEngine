"""Claim: the aggregate root. Owns the document reference, its redacted text and
the lifecycle every other component reads and advances.
"""

import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from ecet.domain.errors import (
    InvalidObjectKey,
    InvalidTransition,
    PdfTooLarge,
    TenantMismatch,
)
from ecet.domain.evaluation import DeterministicResult, Evaluation
from ecet.domain.ids import TENANT_ID_BODY, ClaimId, PolicyId, TenantId, TenantIdField

__all__ = [
    "OBJECT_KEY_RE",
    "Claim",
    "ClaimId",
    "ClaimStatus",
    "RedactedText",
    "SourceObject",
]

OBJECT_KEY_RE = re.compile(rf"^tenants/(?P<tenant>{TENANT_ID_BODY})/claims/[^/]+\.pdf$")


class SourceObject(BaseModel):
    """The S3 object the claim came from. `(bucket, key, etag)` is the idempotency
    key (ADR-006)."""

    model_config = ConfigDict(frozen=True)

    bucket: str = Field(min_length=1)
    key: str
    etag: str = Field(min_length=1)
    size: int = Field(gt=0)

    @field_validator("key")
    @classmethod
    def _key_matches_the_tenant_layout(cls, value: str) -> str:
        if OBJECT_KEY_RE.match(value) is None:
            raise InvalidObjectKey(
                f"expected tenants/{{tenant_id}}/claims/{{name}}.pdf, got {value!r}"
            )
        return value

    def tenant_id(self) -> TenantId:
        match = OBJECT_KEY_RE.match(self.key)
        if match is None:  # pragma: no cover - the field validator guarantees a match
            raise InvalidObjectKey(self.key)
        return TenantId(match.group("tenant"))

    def ensure_size_within(self, max_bytes: int) -> None:
        """`max_bytes` is injected (config `ECET_MAX_PDF_BYTES`); the domain reads no config."""
        if self.size > max_bytes:
            raise PdfTooLarge(f"{self.size} bytes exceeds the {max_bytes} byte limit")


class RedactedText(BaseModel):
    """Post-redaction text plus the audit trail of what was removed.

    ADR-001: raw text is never a field on any domain model — this is the only text
    a claim ever holds.
    """

    model_config = ConfigDict(frozen=True)

    text: str
    entity_counts: dict[str, int] = Field(default_factory=dict)
    redactor: str = Field(min_length=1)


class ClaimStatus(StrEnum):
    RECEIVED = "RECEIVED"
    EXTRACTED = "EXTRACTED"
    REDACTED = "REDACTED"
    POLICIES_ATTACHED = "POLICIES_ATTACHED"
    QUEUED = "QUEUED"
    EVALUATED = "EVALUATED"
    APPROVED_AUTO = "APPROVED_AUTO"
    REVIEW_PENDING = "REVIEW_PENDING"
    REVIEW_RESOLVED = "REVIEW_RESOLVED"
    EXTRACTION_FAILED = "EXTRACTION_FAILED"
    NO_POLICIES = "NO_POLICIES"
    EVALUATION_FAILED = "EVALUATION_FAILED"
    NOTIFY_FAILED = "NOTIFY_FAILED"

    def can_transition_to(self, nxt: "ClaimStatus") -> bool:
        return nxt in _ALLOWED[self]


FAILURE_STATUSES = frozenset(
    {
        ClaimStatus.EXTRACTION_FAILED,
        ClaimStatus.NO_POLICIES,
        ClaimStatus.EVALUATION_FAILED,
        ClaimStatus.NOTIFY_FAILED,
    }
)

_ALLOWED: dict[ClaimStatus, frozenset[ClaimStatus]] = {
    ClaimStatus.RECEIVED: frozenset({ClaimStatus.EXTRACTED, ClaimStatus.EXTRACTION_FAILED}),
    ClaimStatus.EXTRACTED: frozenset({ClaimStatus.REDACTED}),
    ClaimStatus.REDACTED: frozenset({ClaimStatus.POLICIES_ATTACHED, ClaimStatus.NO_POLICIES}),
    # POLICIES_ATTACHED -> REVIEW_PENDING is the ADR-002 deterministic short-circuit.
    ClaimStatus.POLICIES_ATTACHED: frozenset({ClaimStatus.QUEUED, ClaimStatus.REVIEW_PENDING}),
    ClaimStatus.QUEUED: frozenset({ClaimStatus.EVALUATED, ClaimStatus.EVALUATION_FAILED}),
    # EVALUATED/REVIEW_PENDING -> NOTIFY_FAILED: delivery failed before the claim ever
    # reached APPROVED_AUTO/REVIEW_RESOLVED, which both mean "webhook delivered"
    # (UC-07 step 2, UC-09c step 4).
    ClaimStatus.EVALUATED: frozenset(
        {ClaimStatus.APPROVED_AUTO, ClaimStatus.REVIEW_PENDING, ClaimStatus.NOTIFY_FAILED}
    ),
    ClaimStatus.APPROVED_AUTO: frozenset({ClaimStatus.NOTIFY_FAILED}),
    ClaimStatus.REVIEW_PENDING: frozenset({ClaimStatus.REVIEW_RESOLVED, ClaimStatus.NOTIFY_FAILED}),
    ClaimStatus.REVIEW_RESOLVED: frozenset({ClaimStatus.NOTIFY_FAILED}),
    # Operator retry: POST /v1/claims/{id}/retry-notify.
    ClaimStatus.NOTIFY_FAILED: frozenset({ClaimStatus.APPROVED_AUTO, ClaimStatus.REVIEW_RESOLVED}),
    # UC-06 opens a review task on invalid LLM output; a human still resolves it.
    ClaimStatus.EVALUATION_FAILED: frozenset({ClaimStatus.REVIEW_RESOLVED}),
    ClaimStatus.EXTRACTION_FAILED: frozenset(),
    ClaimStatus.NO_POLICIES: frozenset(),
}


class Claim(BaseModel):
    """One claim document travelling the pipeline."""

    model_config = ConfigDict(validate_assignment=True)

    id: ClaimId
    tenant_id: TenantIdField
    source: SourceObject
    status: ClaimStatus = ClaimStatus.RECEIVED
    redacted: RedactedText | None = None
    policy_ids: list[PolicyId] = Field(default_factory=list)
    deterministic: DeterministicResult | None = None
    evaluation: Evaluation | None = None
    failure_reason: str | None = None
    notification_attempts: int = Field(default=0, ge=0)
    last_notify_error: str | None = None
    created_at: AwareDatetime
    updated_at: AwareDatetime

    @model_validator(mode="after")
    def _tenant_matches_the_object_key(self) -> Self:
        """Repositories filter on `tenant_id`, object storage reads by key — a mismatch
        is an undetectable cross-tenant read."""
        if self.tenant_id != self.source.tenant_id():
            raise TenantMismatch(
                f"claim tenant {self.tenant_id!r} != key tenant {self.source.tenant_id()!r}"
            )
        return self

    def transition(
        self,
        nxt: ClaimStatus,
        *,
        reason: str | None = None,
        now: datetime | None = None,
    ) -> None:
        """Advance the lifecycle. Raises `InvalidTransition` on an illegal edge or on a
        failure state with no reason. Every transition bumps `updated_at`."""
        if not self.status.can_transition_to(nxt):
            raise InvalidTransition(f"{self.status} -> {nxt} is not an allowed transition")
        if nxt in FAILURE_STATUSES and not (reason or "").strip():
            raise InvalidTransition(f"transition to {nxt} requires a reason")
        self.status = nxt
        # A non-failure status is a recovery: the old reason no longer describes the claim.
        self.failure_reason = reason if nxt in FAILURE_STATUSES else None
        self.updated_at = now if now is not None else datetime.now(UTC)
