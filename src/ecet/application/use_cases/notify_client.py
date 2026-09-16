"""UC-08 NotifyClient.

One `execute` is one delivery attempt from the claim's point of view — the adapter's
internal retries are invisible here, so `notification_attempts` counts the times the
system tried to tell the client, not the number of HTTP requests. `execute` does not
mutate the claim: the caller (UC-07, UC-09c, or the operator retry) owns the unit of
work, reads the tenant itself, and applies the resulting `Delivery` to whichever claim
it ends up saving.
"""

from dataclasses import dataclass

import structlog

from ecet.application.errors import WebhookError, WebhookPermanentError
from ecet.application.notifications import ClientNotification, DecidedBy, Outcome
from ecet.application.ports.clock import Clock
from ecet.application.ports.webhook_client import WebhookClient
from ecet.domain.claim import Claim
from ecet.domain.errors import TenantNotFound
from ecet.domain.evaluation import Decision
from ecet.domain.ids import TenantId
from ecet.domain.ports.tenant_repository import TenantRepository
from ecet.domain.tenant import Tenant

log = structlog.get_logger(__name__)

#: `failure_reason` tokens for a delivery that did not happen. Short tokens, matching the
#: `EXTRACTION_FAILED` convention; the exception detail lives on `Claim.last_notify_error`.
REJECTED = "webhook_rejected"
UNREACHABLE = "webhook_unreachable"
TENANT_INACTIVE = "tenant_inactive"


@dataclass(frozen=True)
class Delivery:
    """What one attempt did, as a value.

    The claim is not mutated here: the caller delivers *outside* its unit of work and
    then saves a claim it re-read inside a second one, so the fields have to be
    applied to that object, not to the one the payload was built from.

    `attempted` is `False` only for a tenant deactivated before delivery: nothing left
    the process, and `notification_attempts` counts the times the system *tried* to
    tell the client, not every reason a claim can park.
    """

    failure_reason: str | None
    error: str | None
    attempted: bool = True

    @property
    def succeeded(self) -> bool:
        return self.failure_reason is None

    def apply_to(self, claim: Claim) -> None:
        if self.attempted:
            claim.notification_attempts += 1
        claim.last_notify_error = self.error


def tenant_inactive(error: TenantNotFound) -> Delivery:
    """A tenant deactivated since the claim was ingested. Nothing was sent, and the
    claim parks under the same token as any other undelivered decision."""
    return Delivery(
        failure_reason=TENANT_INACTIVE,
        error=f"{type(error).__name__}: {error}",
        attempted=False,
    )


async def tenant_for_delivery(
    tenants: TenantRepository, tenant_id: TenantId
) -> Tenant | TenantNotFound:
    """The tenant a delivery goes to, read while the caller's unit of work is still open
    — the delivery itself happens after it closes. A tenant deactivated in the meantime
    is not an error here: it is a delivery that will never happen, and the caller parks
    the claim under `tenant_inactive`. Returned as one value rather than an
    `(ok, error)` pair so the caller narrows with `isinstance` instead of an `assert`."""
    try:
        return await tenants.get(tenant_id)
    except TenantNotFound as error:
        return error


class NotifyClient:
    def __init__(self, webhook: WebhookClient, clock: Clock) -> None:
        self._webhook = webhook
        self._clock = clock

    async def execute(
        self,
        claim: Claim,
        tenant: Tenant,
        *,
        outcome: Decision,
        confidence: float,
        decided_by: DecidedBy,
    ) -> ClientNotification:
        evaluation = claim.evaluation
        payload = ClientNotification(
            claim_id=claim.id,
            tenant_id=claim.tenant_id,
            source_key=claim.source.key,
            # `Decision` is a StrEnum, so its value is the literal the payload declares.
            outcome=cast_outcome(outcome),
            confidence=confidence,
            decided_by=decided_by,
            matched_policy_id=evaluation.matched_policy_id if evaluation else None,
            cited_codes=[code.code for code in evaluation.cited_codes] if evaluation else [],
            rationale=evaluation.rationale if evaluation else "",
            evidence_missing=list(evaluation.evidence_missing) if evaluation else [],
            decided_at=self._clock.now(),
        )
        await self._webhook.deliver(tenant, payload)
        log.info(
            "notify.delivered",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            outcome=payload.outcome,
            decided_by=decided_by,
        )
        return payload

    async def attempt(
        self,
        claim: Claim,
        tenant: Tenant,
        *,
        outcome: Decision,
        confidence: float,
        decided_by: DecidedBy,
    ) -> Delivery:
        """`execute` for a caller that parks the claim instead of propagating. UC-07,
        UC-09c and the operator retry all park a failed delivery the same way, so the
        mapping lives here once."""
        try:
            await self.execute(
                claim, tenant, outcome=outcome, confidence=confidence, decided_by=decided_by
            )
        except WebhookPermanentError as error:
            return self._failed(claim, REJECTED, error)
        except WebhookError as error:
            return self._failed(claim, UNREACHABLE, error)
        return Delivery(failure_reason=None, error=None)

    @staticmethod
    def _failed(claim: Claim, reason: str, error: Exception) -> Delivery:
        log.warning(
            "notify.failed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            error=type(error).__name__,
        )
        return Delivery(failure_reason=reason, error=f"{type(error).__name__}: {error}")


def cast_outcome(decision: Decision) -> Outcome:
    """`Decision` and `Outcome` list the same three names; this is the one place the
    static types are joined, so a new `Decision` member fails here rather than in a
    payload."""
    outcome: Outcome
    match decision:
        case Decision.MEETS_NECESSITY:
            outcome = "MEETS_NECESSITY"
        case Decision.DOES_NOT_MEET:
            outcome = "DOES_NOT_MEET"
        case Decision.INSUFFICIENT_EVIDENCE:
            outcome = "INSUFFICIENT_EVIDENCE"
    return outcome
