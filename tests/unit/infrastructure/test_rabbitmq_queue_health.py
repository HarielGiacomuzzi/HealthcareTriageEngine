"""`/readyz` trusts `RabbitMqEvaluationQueue.is_healthy`. aio-pika keeps a robust
connection "open" (`is_closed` False) while it retries, so every one of the four
conditions is load-bearing — each case below is the one that catches its removal."""

from typing import Any, cast

import pytest

from ecet.infrastructure.queue.rabbitmq import RabbitMqEvaluationQueue


class _Connection:
    def __init__(self, *, is_closed: bool = False, reconnecting: bool = False) -> None:
        self.is_closed = is_closed
        self.reconnecting = reconnecting


def queue_with(connection: _Connection | None, *, exchange: bool = True) -> RabbitMqEvaluationQueue:
    queue = RabbitMqEvaluationQueue("amqp://unused/")
    queue._connection = cast(Any, connection)
    queue._exchange = cast(Any, object() if exchange else None)
    return queue


@pytest.mark.parametrize(
    ("queue", "healthy"),
    [
        (queue_with(_Connection()), True),
        (queue_with(None), False),
        (queue_with(_Connection(is_closed=True)), False),
        (queue_with(_Connection(reconnecting=True)), False),
        (queue_with(_Connection(), exchange=False), False),
    ],
    ids=["open", "never-started", "closed", "reconnecting", "no-exchange"],
)
async def test_is_healthy_needs_an_open_settled_connection_and_an_exchange(
    queue: RabbitMqEvaluationQueue, healthy: bool
) -> None:
    assert await queue.is_healthy() is healthy
