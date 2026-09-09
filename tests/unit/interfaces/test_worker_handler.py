"""The ack/nack policy is a pure function so that "does a 429 requeue?" is answerable
without a broker, a database or a vendor."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError
from tests.pii import assert_no_pii

from ecet.application.errors import (
    LLMInvalidOutput,
    LLMPermanentError,
    LLMTransientError,
    QueuePublishError,
    WebhookTransientError,
)
from ecet.application.messages import EvaluationMessage
from ecet.domain.errors import ClaimNotFound, InvalidTransition
from ecet.domain.ids import ClaimId, TenantId
from ecet.interfaces.worker.handler import (
    AckAction,
    WorkerMessageHandler,
    classify,
    should_requeue,
)

NOW = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)


def build_message() -> EvaluationMessage:
    return EvaluationMessage(
        message_id=uuid4(),
        claim_id=ClaimId(uuid4()),
        tenant_id=TenantId("tenant-a"),
        redacted_text="Patient <PERSON> with M54.5.",
        policies=[],
        deterministic_verdict="PASS",
        found_codes=["M54.5"],
        enqueued_at=NOW,
    )


def test_no_error_acks() -> None:
    assert classify(None) is AckAction.ACK


@pytest.mark.parametrize(
    "error",
    [
        LLMTransientError("429"),
        QueuePublishError("no confirm"),
        OperationalError("SELECT 1", {}, Exception("connection reset")),
        ConnectionError("reset"),
        TimeoutError("slow"),
    ],
)
def test_recoverable_failures_requeue(error: BaseException) -> None:
    assert classify(error) is AckAction.REQUEUE
    assert should_requeue(error) is True


@pytest.mark.parametrize(
    "error",
    [
        LLMPermanentError("401"),
        LLMInvalidOutput("no tool call"),
        WebhookTransientError("exhausted"),
        InvalidTransition("QUEUED -> APPROVED_AUTO"),
        ClaimNotFound("nope"),
        IntegrityError("INSERT", {}, Exception("duplicate key")),
        ValueError("bug"),
    ],
)
def test_everything_else_goes_to_the_dead_letter_queue(error: BaseException) -> None:
    # WebhookTransientError is in this list on purpose: UC-07 catches it and parks the
    # claim in NOTIFY_FAILED, so if one ever escapes to here it is a bug, not a retry.
    assert classify(error) is AckAction.DLQ
    assert should_requeue(error) is False


class _Evaluate:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[EvaluationMessage] = []
        self.error = error

    async def execute(self, message: EvaluationMessage) -> None:
        self.calls.append(message)
        if self.error is not None:
            raise self.error


async def test_the_handler_delegates_to_the_use_case() -> None:
    evaluate = _Evaluate()
    message = build_message()

    await WorkerMessageHandler(evaluate).handle(message)

    assert evaluate.calls == [message]
    assert_no_pii(evaluate.calls[0].model_dump_json())


async def test_the_handler_re_raises_so_the_consumer_can_classify() -> None:
    evaluate = _Evaluate(LLMTransientError("429"))
    message = build_message()

    with pytest.raises(LLMTransientError):
        await WorkerMessageHandler(evaluate).handle(message)

    assert_no_pii(message.model_dump_json())
