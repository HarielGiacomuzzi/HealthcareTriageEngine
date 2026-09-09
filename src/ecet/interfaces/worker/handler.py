"""The delivery policy, as a pure function.

Everything the worker does with a message reduces to one of three answers, and the
mapping is a policy decision worth reading in one place: **requeue only what a
retry could plausibly fix.** A vendor 429 or a dropped database connection, yes. A
401, a schema violation, a bug — no: those return identically forever and would spin
against the delivery limit before landing in the DLQ anyway.

`WebhookTransientError` is deliberately *not* requeued. UC-07 already catches it and
parks the claim in `NOTIFY_FAILED`; one escaping to here means a code path forgot to,
and requeueing would re-run a whole LLM evaluation to retry an HTTP POST.
"""

import time
from enum import StrEnum
from typing import Protocol

import structlog
from sqlalchemy.exc import InterfaceError, OperationalError

from ecet.application.errors import LLMTransientError, QueuePublishError
from ecet.application.messages import EvaluationMessage

log = structlog.get_logger(__name__)


class AckAction(StrEnum):
    ACK = "ACK"
    REQUEUE = "REQUEUE"
    DLQ = "DLQ"


#: `OperationalError`/`InterfaceError` are SQLAlchemy's connection-level failures; a
#: statement-level failure (IntegrityError, ProgrammingError) is not in the list.
REQUEUE_ERRORS: tuple[type[BaseException], ...] = (
    LLMTransientError,
    QueuePublishError,
    OperationalError,
    InterfaceError,
    ConnectionError,
    TimeoutError,
)


def classify(error: BaseException | None) -> AckAction:
    if error is None:
        return AckAction.ACK
    if isinstance(error, REQUEUE_ERRORS):
        return AckAction.REQUEUE
    return AckAction.DLQ


def should_requeue(error: BaseException) -> bool:
    return classify(error) is AckAction.REQUEUE


class _Evaluator(Protocol):
    async def execute(self, message: EvaluationMessage) -> None: ...


class WorkerMessageHandler:
    """Runs the use case and emits the per-message log line the worker spec asks for.
    It re-raises: the consumer, not the handler, owns the ack."""

    def __init__(self, evaluate: _Evaluator) -> None:
        self._evaluate = evaluate

    async def handle(self, message: EvaluationMessage) -> None:
        started = time.perf_counter()
        try:
            await self._evaluate.execute(message)
        except BaseException as error:
            log.warning(
                "worker.message_failed",
                claim_id=str(message.claim_id),
                tenant_id=str(message.tenant_id),
                outcome=classify(error).value,
                duration_ms=int((time.perf_counter() - started) * 1000),
                error=type(error).__name__,
            )
            raise
        log.info(
            "worker.message_handled",
            claim_id=str(message.claim_id),
            tenant_id=str(message.tenant_id),
            outcome=AckAction.ACK.value,
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
