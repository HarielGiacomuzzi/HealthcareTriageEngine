"""UC-08 NotifyClient.

One `execute` is one delivery attempt from the claim's point of view — the adapter's
internal retries are invisible here, so `notification_attempts` counts the times the
system tried to tell the client, not the number of HTTP requests. The claim is
mutated but not saved: the caller (UC-07, UC-09c, or the operator retry) owns the unit of
work and decides what the failure means for the claim's status.
"""

import structlog

from ecet.application.errors import WebhookError, WebhookPermanentError
from ecet.application.notifications import ClientNotification, DecidedBy, Outcome
from ecet.application.ports.clock import Clock
from ecet.application.ports.webhook_client import WebhookClient
from ecet.domain.claim import Claim
from ecet.domain.errors import TenantNotFound
from ecet.domain.evaluation import Decision
from ecet.domain.ports.tenant_repository import TenantRepository

log = structlog.get_logger(__name__)

#: `failure_reason` tokens for a delivery that did not happen. Short tokens, matching the
#: `EXTRACTION_FAILED` convention; the exception detail lives on `Claim.last_notify_error`.
REJECTED = "webhook_rejected"
UNREACHABLE = "webhook_unreachable"
TENANT_INACTIVE = "tenant_inactive"


class NotifyClient:
    def __init__(self, tenants: TenantRepository, webhook: WebhookClient, clock: Clock) -> None:
        self._tenants = tenants
        self._webhook = webhook
        self._clock = clock

    async def execute(
        self,
        claim: Claim,
        *,
        outcome: Decision,
        confidence: float,
        decided_by: DecidedBy,
    ) -> ClientNotification:
        tenant = await self._tenants.get(claim.tenant_id)
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

        claim.notification_attempts += 1
        try:
            await self._webhook.deliver(tenant, payload)
        except WebhookError as error:
            # The detail goes on the claim for an operator to read; `failure_reason`
            # stays a short token, set by the caller.
            claim.last_notify_error = f"{type(error).__name__}: {error}"
            log.warning(
                "notify.failed",
                claim_id=str(claim.id),
                tenant_id=str(claim.tenant_id),
                attempts=claim.notification_attempts,
                error=type(error).__name__,
            )
            raise
        claim.last_notify_error = None
        log.info(
            "notify.delivered",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            outcome=payload.outcome,
            decided_by=decided_by,
            attempts=claim.notification_attempts,
        )
        return payload

    async def attempt(
        self,
        claim: Claim,
        *,
        outcome: Decision,
        confidence: float,
        decided_by: DecidedBy,
    ) -> str | None:
        """`execute` for a caller that parks the claim instead of propagating: `None`
        when the webhook was delivered, otherwise the `failure_reason` token to park it
        under. UC-07, UC-09c and the operator retry all park a failed delivery the same
        way, so the mapping lives here once."""
        try:
            await self.execute(claim, outcome=outcome, confidence=confidence, decided_by=decided_by)
        except TenantNotFound as error:
            # The tenant is read before any delivery is attempted, and the repository
            # refuses one deactivated since the claim was ingested.
            claim.last_notify_error = f"{type(error).__name__}: {error}"
            return TENANT_INACTIVE
        except WebhookPermanentError:
            return REJECTED
        except WebhookError:
            return UNREACHABLE
        return None


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
