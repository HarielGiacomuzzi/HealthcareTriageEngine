"""UC-05 EnqueueEvaluation.

The hand-off point. Everything the worker needs travels in the message — nothing it
needs is re-read from the API's memory, and nothing the tenant owns (webhook URL,
HMAC secret) travels with it.
"""

from collections.abc import Sequence
from typing import Literal, cast
from uuid import UUID, uuid4

from ecet.application.messages import EvaluationMessage, PolicySnapshot
from ecet.application.ports.clock import Clock
from ecet.application.ports.evaluation_queue import EvaluationQueue
from ecet.domain.claim import Claim
from ecet.domain.evaluation import Verdict
from ecet.domain.policy import Policy
from ecet.domain.rules import extract_icd10_codes


class EnqueueEvaluation:
    def __init__(self, queue: EvaluationQueue, clock: Clock) -> None:
        self._queue = queue
        self._clock = clock

    async def execute(self, claim: Claim, policies: Sequence[Policy]) -> UUID:
        redacted = claim.redacted
        if redacted is None:
            raise ValueError(f"claim {claim.id} has no redacted text to enqueue")
        deterministic = claim.deterministic
        if deterministic is None:
            raise ValueError(f"claim {claim.id} has no deterministic result to enqueue")
        if deterministic.verdict is Verdict.REJECT:
            raise ValueError(f"claim {claim.id} is a deterministic REJECT and must not be queued")

        message = EvaluationMessage(
            message_id=uuid4(),
            claim_id=claim.id,
            tenant_id=claim.tenant_id,
            redacted_text=redacted.text,
            entity_counts=dict(redacted.entity_counts),
            policies=[PolicySnapshot.of(policy) for policy in policies],
            deterministic_verdict=cast(
                Literal["PASS", "UNCERTAIN"],
                deterministic.verdict.value,
            ),
            # Recomputed rather than carried on `DeterministicResult`, which records
            # check outcomes, not the codes themselves.
            found_codes=[code.code for code in extract_icd10_codes(redacted.text)],
            enqueued_at=self._clock.now(),
        )
        await self._queue.publish(message)
        return message.message_id
