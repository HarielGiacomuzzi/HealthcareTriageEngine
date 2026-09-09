"""The outbound contract: what a tenant's system receives when a claim is decided.

No claim text, deliberately (UC-08): the client already owns the document, so sending
its de-identified body back would put clinical content on a wire that does not need
it. The signature and delivery headers are the adapter's job, not this model's.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from ecet.domain.ids import ClaimId, PolicyId, TenantIdField

__all__ = ["ClientNotification", "DecidedBy", "Outcome"]

DecidedBy = Literal["auto", "human"]
Outcome = Literal["MEETS_NECESSITY", "DOES_NOT_MEET", "INSUFFICIENT_EVIDENCE"]


class ClientNotification(BaseModel):
    model_config = ConfigDict(frozen=True)

    event: Literal["claim.triaged"] = "claim.triaged"
    claim_id: ClaimId
    tenant_id: TenantIdField
    source_key: str
    outcome: Outcome
    confidence: float = Field(ge=0.0, le=1.0)
    decided_by: DecidedBy
    matched_policy_id: PolicyId | None = None
    cited_codes: list[str] = Field(default_factory=list)
    rationale: str = ""
    evidence_missing: list[str] = Field(default_factory=list)
    decided_at: datetime
