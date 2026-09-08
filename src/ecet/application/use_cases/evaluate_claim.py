"""UC-06 EvaluateClaim — the worker-side orchestrator.

The contract with the consumer is expressed by control flow, not by a return value:
**raising means requeue, returning means ack.** Every branch that a retry could not
improve — an unknown claim, a claim already past `QUEUED`, a vendor answer that will
be identical next time — returns. Only a transient vendor failure escapes.

The message is the source of truth for what the model sees. Policies travel as a
snapshot taken at enqueue time (UC-05), so a policy edited while the message sat in
the queue cannot silently change the basis of a decision. The claim row is still read,
because the status check, the tenant check and the write all need it.

The one thing re-read from the database is the ICD-10 catalogue: `domain/rules.py`
extracts codes by pattern, which false-positives on clinical prose ("Vitamin B12"),
and the seeded catalogue is what keeps those out of the prompt.
"""

from collections.abc import Callable

import structlog

from ecet.application.errors import LLMInvalidOutput, LLMPermanentError, LLMTransientError
from ecet.application.messages import EvaluationMessage
from ecet.application.ports.clock import Clock
from ecet.application.ports.llm_gateway import EvaluationRequest, LLMGateway
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.ports.webhook_client import WebhookClient
from ecet.application.use_cases.human_review import RequestHumanReview
from ecet.application.use_cases.route_decision import RouteDecision
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import ClaimNotFound
from ecet.domain.evaluation import ReviewReason

log = structlog.get_logger(__name__)

INVALID_OUTPUT = "llm_invalid_output"
PERMANENT_ERROR = "llm_permanent_error"


class EvaluateClaim:
    def __init__(
        self,
        *,
        uow_factory: Callable[[], UnitOfWork],
        llm: LLMGateway,
        webhook: WebhookClient,
        clock: Clock,
        threshold: float,
        prompt_version: str,
    ) -> None:
        self._uow_factory = uow_factory
        self._llm = llm
        self._webhook = webhook
        self._clock = clock
        self._threshold = threshold
        self._prompt_version = prompt_version

    async def execute(self, message: EvaluationMessage) -> None:
        async with self._uow_factory() as uow:
            claim = await self._load(uow, message)
            if claim is None:
                return

            request = await self._build_request(uow, claim, message)
            try:
                evaluation = await self._llm.evaluate(request)
            except LLMTransientError:
                # The only escape hatch: the consumer nacks with requeue and RabbitMQ's
                # x-delivery-limit eventually sends it to the DLQ. Nothing is saved, so
                # the claim is still QUEUED for the redelivery.
                raise
            except (LLMInvalidOutput, LLMPermanentError) as error:
                await self._fail(uow, claim, error)
                return

            claim.evaluation = evaluation
            claim.transition(ClaimStatus.EVALUATED, now=self._clock.now())
            log.info(
                "claim.transition",
                claim_id=str(claim.id),
                tenant_id=str(claim.tenant_id),
                to=ClaimStatus.EVALUATED.value,
            )
            await uow.claims.save(claim)

            route = RouteDecision(
                uow=uow,
                webhook=self._webhook,
                clock=self._clock,
                threshold=self._threshold,
            )
            await route.execute(claim)
            await uow.commit()

    async def _load(self, uow: UnitOfWork, message: EvaluationMessage) -> Claim | None:
        """The three ack-and-forget cases, in the order they can be checked cheaply."""
        try:
            claim = await uow.claims.get(message.claim_id)
        except ClaimNotFound:
            log.error("evaluate.claim_missing", claim_id=str(message.claim_id))
            return None

        if claim.tenant_id != message.tenant_id:
            # Nothing legitimate produces this. Refusing it keeps a forged or corrupted
            # message from evaluating one tenant's claim against another's policies.
            log.error(
                "evaluate.tenant_mismatch",
                claim_id=str(claim.id),
                tenant_id=str(claim.tenant_id),
                message_tenant_id=str(message.tenant_id),
            )
            return None

        if claim.status is not ClaimStatus.QUEUED:
            log.info(
                "evaluate.skipped_duplicate",
                claim_id=str(claim.id),
                tenant_id=str(claim.tenant_id),
                status=claim.status.value,
            )
            return None
        return claim

    async def _build_request(
        self, uow: UnitOfWork, claim: Claim, message: EvaluationMessage
    ) -> EvaluationRequest:
        known = {code.code for code in await uow.icd10_codes.known_codes()}
        return EvaluationRequest(
            claim_id=claim.id,
            redacted_text=message.redacted_text,
            policies=list(message.policies),
            found_codes=[code for code in message.found_codes if code in known],
            prompt_version=self._prompt_version,
        )

    async def _fail(self, uow: UnitOfWork, claim: Claim, error: Exception) -> None:
        reason = INVALID_OUTPUT if isinstance(error, LLMInvalidOutput) else PERMANENT_ERROR
        claim.transition(ClaimStatus.EVALUATION_FAILED, reason=reason, now=self._clock.now())
        log.warning(
            "claim.failed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=ClaimStatus.EVALUATION_FAILED.value,
            reason=reason,
            error=type(error).__name__,
        )
        await RequestHumanReview(uow.review_tasks, self._clock).execute(
            claim, ReviewReason.EVALUATION_FAILED
        )
        await uow.claims.save(claim)
        await uow.commit()
