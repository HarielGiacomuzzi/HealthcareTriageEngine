"""The `claims.evaluate` wire contract.

Pydantic on both ends: the API builds it, the worker validates it. `schema_version`
is a `Literal[1]`, so a future version 2 fails validation loudly on an old consumer
instead of being half-understood.

Nothing here may carry raw text, a webhook URL or a secret — the queue is the first
place a claim leaves the API process (ADR-001).
"""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from ecet.domain.ids import ClaimId, PolicyId, TenantIdField
from ecet.domain.policy import Policy


class PolicySnapshot(BaseModel):
    """A policy frozen at enqueue time. The worker evaluates against this, not against
    a fresh read, so a policy edited mid-flight cannot change a claim's basis."""

    model_config = ConfigDict(frozen=True)

    id: PolicyId
    name: str
    version: int
    covered_codes: list[str]
    excluded_codes: list[str]
    criteria_text: str
    required_evidence: list[str]

    @classmethod
    def of(cls, policy: Policy) -> "PolicySnapshot":
        # `covered_codes`/`excluded_codes` are sets on the domain model; sorting makes
        # the serialised message byte-stable, which keeps message diffs readable.
        return cls(
            id=policy.id,
            name=policy.name,
            version=policy.version,
            covered_codes=sorted(code.code for code in policy.covered_codes),
            excluded_codes=sorted(code.code for code in policy.excluded_codes),
            criteria_text=policy.criteria_text,
            required_evidence=list(policy.required_evidence),
        )


class EvaluationMessage(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: Literal[1] = 1
    message_id: UUID
    claim_id: ClaimId
    tenant_id: TenantIdField
    redacted_text: str
    entity_counts: dict[str, int] = Field(default_factory=dict)
    policies: list[PolicySnapshot]
    #: `REJECT` never reaches the queue — it short-circuits to human review (UC-01 §9).
    deterministic_verdict: Literal["PASS", "UNCERTAIN"]
    found_codes: list[str] = Field(default_factory=list)
    enqueued_at: AwareDatetime
