"""The vendor boundary (ADR-004).

Two models, deliberately not one. `EvaluationOutput` is what a vendor is permitted to
return — the subset a language model can actually author. `Evaluation` is what the
domain stores, and it additionally carries the facts only the adapter knows: which
model answered, how long it took, how many tokens it cost. `to_evaluation` is the one
bridge, so every adapter fills those fields the same way.
"""

from collections.abc import Collection
from typing import Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ecet.application.messages import PolicySnapshot
from ecet.domain.evaluation import Decision, Evaluation
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.policy import Icd10Code

__all__ = ["EvaluationOutput", "EvaluationRequest", "LLMGateway"]


class EvaluationRequest(BaseModel):
    """Everything the model is allowed to see. No tenant, no webhook, no raw text."""

    model_config = ConfigDict(frozen=True)

    claim_id: ClaimId
    redacted_text: str
    policies: list[PolicySnapshot] = Field(default_factory=list)
    found_codes: list[str] = Field(default_factory=list)
    prompt_version: str


class EvaluationOutput(BaseModel):
    """The tool-call schema. `extra="forbid"` makes an invented field a validation
    error rather than silently ignored output."""

    model_config = ConfigDict(extra="forbid")

    decision: Decision
    #: Optional on purpose: the prompt demands it, but a server that omits it must
    #: degrade to INSUFFICIENT_EVIDENCE (evaluation.md §2) rather than fail the claim.
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    matched_policy_id: UUID | None = None
    cited_codes: list[str] = Field(default_factory=list)
    rationale: str = Field(default="", max_length=2000)
    evidence_found: list[str] = Field(default_factory=list)
    evidence_missing: list[str] = Field(default_factory=list)

    def to_evaluation(
        self,
        *,
        model: str,
        prompt_version: str,
        latency_ms: int,
        input_tokens: int,
        output_tokens: int,
        known_policy_ids: Collection[PolicyId] = (),
    ) -> Evaluation:
        """Build the domain model. Two defensive conversions happen here rather than in
        each adapter: a code the model invented that is not ICD-10-shaped is dropped,
        and a `matched_policy_id` that was not in the request is discarded — neither is
        worth failing an otherwise usable evaluation over, and both would otherwise be
        persisted as fact."""
        decision = self.decision
        confidence = self.confidence
        if confidence is None:
            decision, confidence = Decision.INSUFFICIENT_EVIDENCE, 0.0

        matched = self.matched_policy_id
        if matched is not None and known_policy_ids and matched not in set(known_policy_ids):
            matched = None

        return Evaluation(
            decision=decision,
            confidence=confidence,
            matched_policy_id=PolicyId(matched) if matched is not None else None,
            cited_codes=_parse_codes(self.cited_codes),
            rationale=self.rationale,
            evidence_found=list(self.evidence_found),
            evidence_missing=list(self.evidence_missing),
            model=model,
            prompt_version=prompt_version,
            latency_ms=latency_ms,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )


def _parse_codes(codes: list[str]) -> list[Icd10Code]:
    parsed: list[Icd10Code] = []
    for code in codes:
        try:
            parsed.append(Icd10Code(code=code))
        except ValidationError:
            continue
    return parsed


@runtime_checkable
class LLMGateway(Protocol):
    async def evaluate(self, request: EvaluationRequest) -> Evaluation:
        """Raises `LLMTransientError`, `LLMPermanentError` or `LLMInvalidOutput`."""
        ...
