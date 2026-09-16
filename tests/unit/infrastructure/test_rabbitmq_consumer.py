"""Cancellation is not a delivery outcome.

`stop()` closes the connection once `drain_timeout` expires, which cancels whatever is
still in flight. `_on_message` must let that `CancelledError` through rather than run it
past `classify` — the broker redelivers an unacked message on channel close, and
dead-lettering it instead loses the claim. Driven directly because a broker cannot be
asked to cancel a callback at a chosen instant.
"""

import asyncio
from typing import Any, cast

import pytest
import structlog
from structlog.testing import capture_logs
from tests.pii import assert_no_pii
from tests.unit.interfaces.test_worker_handler import build_message

from ecet.application.messages import EvaluationMessage
from ecet.infrastructure.queue.rabbitmq import RabbitMqConsumer


class _Delivery:
    """The slice of `AbstractIncomingMessage` that `_on_message` touches."""

    def __init__(self, body: bytes) -> None:
        self.body = body
        self.headers: dict[str, Any] = {}
        self.message_id = "delivery-1"
        self.settled: list[str] = []

    async def ack(self) -> None:
        self.settled.append("ack")

    async def nack(self, requeue: bool) -> None:
        self.settled.append(f"nack(requeue={requeue})")


def build_consumer(handler: Any) -> RabbitMqConsumer:
    # `should_requeue=False` is what `classify` answers for the bug-shaped failures these
    # two tests raise, so a settled message here means a dead-lettered one.
    return RabbitMqConsumer(
        "amqp://unused/", prefetch=1, handler=handler, should_requeue=lambda _: False
    )


async def test_a_cancelled_handler_propagates_and_settles_nothing() -> None:
    async def handle(message: EvaluationMessage) -> None:
        raise asyncio.CancelledError

    consumer = build_consumer(handle)
    message = build_message()
    delivery = _Delivery(message.model_dump_json().encode("utf-8"))

    with pytest.raises(asyncio.CancelledError):
        await consumer._on_message(cast(Any, delivery))

    # Neither acked nor nacked: the broker redelivers it when the channel closes.
    assert delivery.settled == []
    assert_no_pii(delivery.body.decode("utf-8"))


async def test_an_ordinary_failure_is_still_nacked() -> None:
    # The companion case, so the narrowed `except` cannot regress into catching nothing.
    async def handle(message: EvaluationMessage) -> None:
        raise ValueError("a bug, not a blip")

    message = build_message()
    delivery = _Delivery(message.model_dump_json().encode("utf-8"))

    await build_consumer(handle)._on_message(cast(Any, delivery))

    assert delivery.settled == ["nack(requeue=False)"]
    assert_no_pii(delivery.body.decode("utf-8"))


async def test_the_consumer_binds_the_message_context_for_the_handler() -> None:
    """`request_id`/`message_id`, `claim_id`, `tenant_id` — the observability spec's
    per-message bound context, so a worker line can be grepped by the same id the api
    answered with."""
    seen: dict[str, object] = {}

    async def handle(message: EvaluationMessage) -> None:
        seen.update(structlog.contextvars.get_contextvars())

    consumer = build_consumer(handle)
    message = build_message()
    delivery = _Delivery(message.model_dump_json().encode("utf-8"))
    delivery.headers = {"x-request-id": "req-1"}

    await consumer._on_message(cast(Any, delivery))

    assert seen["request_id"] == "req-1"
    assert seen["claim_id"] == str(message.claim_id)
    assert seen["tenant_id"] == str(message.tenant_id)
    assert seen["message_id"] == str(message.message_id)
    # Cleared afterwards: the next delivery is a different claim.
    assert structlog.contextvars.get_contextvars() == {}


async def test_a_delivery_without_a_request_id_binds_no_null() -> None:
    """A message published outside an HTTP request (a dlq replay) has no id; a
    `request_id: null` field in the logs would be worse than its absence."""
    seen: dict[str, object] = {}

    async def handle(message: EvaluationMessage) -> None:
        seen.update(structlog.contextvars.get_contextvars())

    delivery = _Delivery(build_message().model_dump_json().encode("utf-8"))

    await build_consumer(handle)._on_message(cast(Any, delivery))

    assert "request_id" not in seen


async def test_stop_waits_for_an_in_flight_delivery_to_settle() -> None:
    """The SIGTERM story in compose: stop taking deliveries, let the one in hand finish
    and ack, then close — not close under it."""
    release = asyncio.Event()

    async def handle(message: EvaluationMessage) -> None:
        await release.wait()

    consumer = RabbitMqConsumer(
        "amqp://unused/",
        prefetch=1,
        handler=handle,
        should_requeue=lambda _: True,
        drain_timeout=5.0,
    )
    delivery = _Delivery(build_message().model_dump_json().encode("utf-8"))
    in_flight = asyncio.create_task(consumer._on_message(cast(Any, delivery)))
    await asyncio.sleep(0.01)  # into the handler

    stopping = asyncio.create_task(consumer.stop())
    await asyncio.sleep(0.05)
    assert not stopping.done(), "stop() returned with a delivery still in flight"

    release.set()
    await asyncio.wait_for(stopping, timeout=1.0)
    await in_flight

    assert delivery.settled == ["ack"]


async def test_stop_gives_up_on_a_stuck_delivery_after_the_drain_timeout() -> None:
    async def handle(message: EvaluationMessage) -> None:
        await asyncio.Event().wait()  # never finishes

    consumer = RabbitMqConsumer(
        "amqp://unused/",
        prefetch=1,
        handler=handle,
        should_requeue=lambda _: True,
        drain_timeout=0.05,
    )
    delivery = _Delivery(build_message().model_dump_json().encode("utf-8"))
    in_flight = asyncio.create_task(consumer._on_message(cast(Any, delivery)))
    await asyncio.sleep(0.01)

    with capture_logs() as captured:
        await asyncio.wait_for(consumer.stop(), timeout=1.0)

    (timeout_line,) = [e for e in captured if e["event"] == "consumer.drain_timeout"]
    assert timeout_line["in_flight"] == 1
    assert delivery.settled == []

    in_flight.cancel()
    with pytest.raises(asyncio.CancelledError):
        await in_flight
