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
