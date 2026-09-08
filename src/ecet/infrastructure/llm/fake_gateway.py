"""The default gateway (`ECET_LLM_PROVIDER=fake`).

Not a test double: this is what `docker compose up` runs, so the demo needs no API key
and no network. The rules are substring matches on the redacted note, chosen so the
committed fixtures land on three different routes — `meets` auto-approves, `unclear`
goes to human review, and a note containing "denied" is refused.
"""

import structlog

from ecet.application.ports.llm_gateway import EvaluationRequest
from ecet.domain.evaluation import Decision, Evaluation
from ecet.domain.ids import PolicyId
from ecet.domain.policy import Icd10Code

log = structlog.get_logger(__name__)

#: (needle, decision, confidence, rationale). First match wins.
RULES: tuple[tuple[str, Decision, float, str], ...] = (
    (
        "denied",
        Decision.DOES_NOT_MEET,
        0.92,
        "The note records a denial, which contradicts the policy criteria.",
    ),
    (
        "unclear",
        Decision.INSUFFICIENT_EVIDENCE,
        0.4,
        "The note leaves the required criteria undocumented.",
    ),
)

DEFAULT = (
    Decision.MEETS_NECESSITY,
    0.91,
    "The note documents the criteria required by the matched policy.",
)


class FakeLlmGateway:
    MODEL = "fake-deterministic"

    async def evaluate(self, request: EvaluationRequest) -> Evaluation:
        text = request.redacted_text.lower()
        decision, confidence, rationale = DEFAULT
        for needle, ruled_decision, ruled_confidence, ruled_rationale in RULES:
            if needle in text:
                decision, confidence, rationale = ruled_decision, ruled_confidence, ruled_rationale
                break

        matched: PolicyId | None = None
        if decision is not Decision.INSUFFICIENT_EVIDENCE and request.policies:
            matched = request.policies[0].id

        log.info(
            "llm.evaluated",
            claim_id=str(request.claim_id),
            provider="fake",
            model=self.MODEL,
            decision=decision.value,
            confidence=confidence,
        )
        return Evaluation(
            decision=decision,
            confidence=confidence,
            matched_policy_id=matched,
            cited_codes=[Icd10Code(code=code) for code in request.found_codes],
            rationale=rationale,
            model=self.MODEL,
            prompt_version=request.prompt_version,
        )
