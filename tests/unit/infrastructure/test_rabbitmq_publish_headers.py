"""The publisher stamps the bound `request_id` on the message. Driven against a stub
exchange: what is under test is the header, not the broker."""

from typing import Any

import structlog
from tests.unit.interfaces.test_worker_handler import build_message

from ecet.infrastructure.queue.rabbitmq import RabbitMqEvaluationQueue


class _Exchange:
    def __init__(self) -> None:
        self.published: list[Any] = []

    async def publish(self, message: Any, routing_key: str) -> None:
        self.published.append(message)


def build_queue(exchange: _Exchange) -> RabbitMqEvaluationQueue:
    queue = RabbitMqEvaluationQueue("amqp://unused/")
    queue._exchange = exchange  # type: ignore[assignment]  # the stub is the seam
    return queue


async def test_publish_stamps_the_bound_request_id() -> None:
    exchange = _Exchange()
    structlog.contextvars.bind_contextvars(request_id="req-1")
    try:
        await build_queue(exchange).publish(build_message())
    finally:
        structlog.contextvars.clear_contextvars()

    assert exchange.published[0].headers["x-request-id"] == "req-1"


async def test_publish_outside_a_request_sets_no_header() -> None:
    exchange = _Exchange()

    await build_queue(exchange).publish(build_message())

    assert "x-request-id" not in exchange.published[0].headers
