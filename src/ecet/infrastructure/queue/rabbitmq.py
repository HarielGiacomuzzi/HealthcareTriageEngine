"""`EvaluationQueue` over aio-pika, plus the topology both processes declare.

The topology is declared by whoever connects first — api or worker — and declaring it
twice is a no-op as long as the arguments match, which is why they live in one
constant here rather than being spelled out at each call site.

The queue is a quorum queue with `x-delivery-limit=5`: after five failed deliveries
RabbitMQ itself moves the message to the DLQ, so the worker never has to count
attempts. Phase 4's consumer imports `declare_topology` from this module.
"""

import structlog
from aio_pika import DeliveryMode, Message, connect_robust
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractRobustConnection
from pamqp.common import FieldTable

from ecet.application.errors import QueuePublishError
from ecet.application.messages import EvaluationMessage

log = structlog.get_logger(__name__)

EXCHANGE = "ecet"
DLX = "ecet.dlx"
QUEUE = "claims.evaluate"
DLQ = "claims.evaluate.dlq"
ROUTING_KEY = "claims.evaluate"

QUEUE_ARGUMENTS: FieldTable = {
    "x-queue-type": "quorum",
    "x-dead-letter-exchange": DLX,
    "x-delivery-limit": 5,
}


async def declare_topology(channel: AbstractChannel) -> AbstractExchange:
    """Declare exchanges, queues and bindings. Idempotent. Returns the `ecet` exchange."""
    exchange = await channel.declare_exchange(EXCHANGE, "topic", durable=True)
    dlx = await channel.declare_exchange(DLX, "topic", durable=True)

    dead_letters = await channel.declare_queue(DLQ, durable=True)
    await dead_letters.bind(dlx, routing_key=ROUTING_KEY)

    evaluations = await channel.declare_queue(QUEUE, durable=True, arguments=QUEUE_ARGUMENTS)
    await evaluations.bind(exchange, routing_key=ROUTING_KEY)

    return exchange


class RabbitMqEvaluationQueue:
    """One robust connection per process, opened at startup and closed at shutdown."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._connection: AbstractRobustConnection | None = None
        self._exchange: AbstractExchange | None = None

    async def start(self) -> None:
        self._connection = await connect_robust(self._url)
        # Publisher confirms: without them a publish is fire-and-forget and the
        # "claim stays POLICIES_ATTACHED on failure" contract is unenforceable.
        channel = await self._connection.channel(publisher_confirms=True)
        self._exchange = await declare_topology(channel)
        log.info("queue.connected", exchange=EXCHANGE, queue=QUEUE)

    async def stop(self) -> None:
        self._exchange = None
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def is_healthy(self) -> bool:
        """Used by `/readyz`; never raises."""
        return self._connection is not None and not self._connection.is_closed

    async def publish(self, message: EvaluationMessage) -> None:
        exchange = self._exchange
        if exchange is None:
            raise QueuePublishError("queue connection is not open")
        try:
            await exchange.publish(
                Message(
                    body=message.model_dump_json().encode("utf-8"),
                    content_type="application/json",
                    delivery_mode=DeliveryMode.PERSISTENT,
                    message_id=str(message.message_id),
                    headers={
                        "x-tenant-id": str(message.tenant_id),
                        "x-schema-version": message.schema_version,
                    },
                ),
                routing_key=ROUTING_KEY,
            )
        except Exception as error:  # any broker failure surfaces as one error type here
            raise QueuePublishError(str(error)) from error
        log.info(
            "queue.published",
            claim_id=str(message.claim_id),
            tenant_id=str(message.tenant_id),
            message_id=str(message.message_id),
        )
