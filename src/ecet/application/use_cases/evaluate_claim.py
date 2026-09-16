"""UC-06 EvaluateClaim — the worker-side orchestrator.

The contract with the consumer is expressed by control flow, not by a return value:
**raising means requeue, returning means ack.** Every branch that a retry could not
improve — an unknown claim, a claim already past `QUEUED`, a vendor answer that will
be identical next time — returns. Only a transient vendor failure, or a claim whose
`QUEUED` commit has not landed yet, escapes.

The message is the source of truth for what the model sees. Policies travel as a
snapshot taken at enqueue time (UC-05), so a policy edited while the message sat in
the queue cannot silently change the basis of a decision. The claim row is still read,
because the status check, the tenant check and the write all need it.

The one thing re-read from the database is the ICD-10 catalogue: `domain/rules.py`
extracts codes by pattern, which false-positives on clinical prose ("Vitamin B12"),
and the seeded catalogue is what keeps those out of the prompt.

**Read, call, write.** The vendor call and, for an `AUTO_NOTIFY` route, the webhook
POST used to run inside the same transaction that loaded the claim — a Postgres
connection idle-in-transaction for up to `llm_timeout_s` plus the webhook's own
retries (Phase 4 carry-over #11). A read-only unit of work now gathers everything
those calls need — the claim, the tenant, the request — and closes before either
runs; a second, later unit of work re-reads the claim, re-checks its status, and does
every write. A crash between the two leaves the claim `QUEUED` and the message
unacked, so RabbitMQ redelivers it — the same at-least-once contract Phase 4 already
documents.

The tenant is read in that first, read-only unit of work too — `_tenant` runs for
every message, including a `HUMAN_REVIEW` route that never delivers, because the
route isn't known until the vendor answers and the tenant row has to be read before
that connection closes. That means the snapshot used for the webhook URL and HMAC
secret can be up to `llm_timeout_s` stale by the time the POST goes out: a tenant
deactivated, or with a rotated secret, during the vendor call is delivered against
the row read a minute earlier. Re-reading the tenant in the second unit of work would
only narrow that window, not close it (the POST itself still has to run outside any
transaction), so it isn't done — this paragraph is the fix.
"""

import asyncio
from collections.abc import Callable

import structlog

from ecet.application.errors import (
    ClaimNotYetQueued,
    LLMInvalidOutput,
    LLMPermanentError,
    LLMTransientError,
)
from ecet.application.messages import EvaluationMessage
from ecet.application.ports.clock import Clock
from ecet.application.ports.llm_gateway import EvaluationRequest, LLMGateway
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.ports.webhook_client import WebhookClient
from ecet.application.use_cases.human_review import RequestHumanReview
from ecet.application.use_cases.notify_client import Delivery, NotifyClient, tenant_inactive
from ecet.application.use_cases.route_decision import RouteDecision
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import ClaimNotFound, TenantNotFound
from ecet.domain.evaluation import ReviewReason, Route
from ecet.domain.tenant import Tenant

log = structlog.get_logger(__name__)

INVALID_OUTPUT = "llm_invalid_output"
PERMANENT_ERROR = "llm_permanent_error"

#: How long a message that beat UC-01's `QUEUED` commit is held before it is requeued.
#: Requeue has no delay, so without this five deliveries could all land inside the gap.
NOT_YET_QUEUED_DELAY_S = 0.5


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
        self._prompt_version = prompt_version
        self._router = RouteDecision(clock=clock, threshold=threshold)

    async def execute(self, message: EvaluationMessage) -> None:
        try:
            async with self._uow_factory() as uow:  # read-only: nothing is written here
                claim = await self._load(uow, message)
                if claim is None:
                    return
                tenant_or_error = await self._tenant(uow, claim)
                request = await self._build_request(uow, claim, message)
        except ClaimNotYetQueued:
            # Held with no connection open, so the redelivery reads the api's commit.
            await asyncio.sleep(NOT_YET_QUEUED_DELAY_S)
            raise

        # No connection is held for either of these. The vendor call can take
        # `llm_timeout_s` and the POST its own timeout; both used to run inside the
        # transaction above (Phase 4 carry-over #11).
        try:
            evaluation = await self._llm.evaluate(request)
        except LLMTransientError:
            # The only escape hatch: the consumer nacks with requeue and RabbitMQ's
            # x-delivery-limit eventually sends it to the DLQ. Nothing is saved, so
            # the claim is still QUEUED for the redelivery.
            raise
        except (LLMInvalidOutput, LLMPermanentError) as error:
            await self._fail(message, error)
            return

        # Assigned on the uow #1 claim, not just `fresh`: `NotifyClient.execute` reads
        # `claim.evaluation` to build the payload (matched policy, cited codes,
        # rationale), and that call happens below, before `fresh` exists. The object is
        # discarded after the POST — the write still lands on `fresh` in uow #2.
        claim.evaluation = evaluation

        route = self._router.decide(claim, evaluation)
        delivery: Delivery | None = None
        if route is Route.AUTO_NOTIFY:
            if isinstance(tenant_or_error, TenantNotFound):
                delivery = tenant_inactive(tenant_or_error)
            else:
                delivery = await NotifyClient(self._webhook, self._clock).attempt(
                    claim,
                    tenant_or_error,
                    outcome=evaluation.decision,
                    confidence=evaluation.confidence,
                    decided_by="auto",
                )

        async with self._uow_factory() as uow:  # every write in this use case
            fresh = await self._load(uow, message)
            if fresh is None:
                # Someone else moved it while the vendor was answering. Their write
                # stands; ours is thrown away, and the message is still acked. If a
                # delivery already left the process, that fact would otherwise vanish
                # with no trace in the log or the metrics.
                if delivery is not None and delivery.attempted:
                    log.warning(
                        "evaluate.delivery_discarded",
                        claim_id=str(message.claim_id),
                        tenant_id=str(message.tenant_id),
                        status=await self._current_status(uow, message),
                        delivery_attempted=delivery.attempted,
                        delivery_succeeded=delivery.succeeded,
                    )
                return
            fresh.evaluation = evaluation
            self._advance(fresh, ClaimStatus.EVALUATED)
            if delivery is not None:
                delivery.apply_to(fresh)
            await self._router.apply(uow, fresh, route=route, delivery=delivery)
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

        if claim.status is ClaimStatus.POLICIES_ATTACHED:
            # UC-01 publishes before it commits QUEUED; this message beat that commit.
            log.info(
                "evaluate.not_yet_queued", claim_id=str(claim.id), tenant_id=str(claim.tenant_id)
            )
            raise ClaimNotYetQueued("not_yet_queued")

        if claim.status is not ClaimStatus.QUEUED:
            log.info(
                "evaluate.skipped_duplicate",
                claim_id=str(claim.id),
                tenant_id=str(claim.tenant_id),
                status=claim.status.value,
            )
            return None
        return claim

    async def _tenant(self, uow: UnitOfWork, claim: Claim) -> Tenant | TenantNotFound:
        """Read before the transaction closes, because the delivery happens after it.
        A tenant deactivated since ingestion is not an error here — it is a delivery
        that will never happen, and the claim parks under `tenant_inactive`. Returned
        as one value rather than an `(ok, error)` pair so the caller narrows with
        `isinstance` instead of an `assert`."""
        try:
            return await uow.tenants.get(claim.tenant_id)
        except TenantNotFound as error:
            return error

    async def _current_status(self, uow: UnitOfWork, message: EvaluationMessage) -> str:
        """Only for the `evaluate.delivery_discarded` log line: the claim guard already
        ran and failed inside `_load`, so this is a second, log-only read."""
        try:
            return (await uow.claims.get(message.claim_id)).status.value
        except ClaimNotFound:
            return "unknown"

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

    async def _fail(self, message: EvaluationMessage, error: Exception) -> None:
        """A vendor answer no retry can improve: park the claim and open a review."""
        reason = INVALID_OUTPUT if isinstance(error, LLMInvalidOutput) else PERMANENT_ERROR
        async with self._uow_factory() as uow:
            claim = await self._load(uow, message)
            if claim is None:
                return
            previous = claim.status
            claim.transition(ClaimStatus.EVALUATION_FAILED, reason=reason, now=self._clock.now())
            log.warning(
                "claim.failed",
                claim_id=str(claim.id),
                tenant_id=str(claim.tenant_id),
                to=ClaimStatus.EVALUATION_FAILED.value,
                reason=reason,
                error=type(error).__name__,
                **{"from": previous.value},
            )
            await RequestHumanReview(uow.review_tasks, self._clock).execute(
                claim, ReviewReason.EVALUATION_FAILED
            )
            await uow.claims.save(claim)
            await uow.commit()

    def _advance(self, claim: Claim, status: ClaimStatus) -> None:
        previous = claim.status
        claim.transition(status, now=self._clock.now())
        log.info(
            "claim.transition",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=status.value,
            **{"from": previous.value},
        )
