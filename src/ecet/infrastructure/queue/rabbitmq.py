"""`EvaluationQueue` over aio-pika, plus the topology both processes declare.

The topology is declared by whoever connects first — api or worker — and declaring it
twice is a no-op as long as the arguments match, which is why they live in one
constant here rather than being spelled out at each call site.

The queue is a quorum queue with `x-delivery-limit=5`: after five failed deliveries
RabbitMQ itself moves the message to the DLQ, so the worker never has to count
attempts. Phase 4's consumer imports `declare_topology` from this module.
"""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import NamedTuple

import structlog
from aio_pika import DeliveryMode, Message, connect_robust
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)
from pamqp.common import FieldTable
from pydantic import ValidationError

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


class Topology(NamedTuple):
    exchange: AbstractExchange
    queue: AbstractQueue


async def declare_topology(channel: AbstractChannel) -> Topology:
    """Declare exchanges, queues and bindings. Idempotent. Both processes call it."""
    exchange = await channel.declare_exchange(EXCHANGE, "topic", durable=True)
    dlx = await channel.declare_exchange(DLX, "topic", durable=True)

    dead_letters = await channel.declare_queue(DLQ, durable=True)
    await dead_letters.bind(dlx, routing_key=ROUTING_KEY)

    evaluations = await channel.declare_queue(QUEUE, durable=True, arguments=QUEUE_ARGUMENTS)
    await evaluations.bind(exchange, routing_key=ROUTING_KEY)

    return Topology(exchange=exchange, queue=evaluations)


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
        self._exchange = (await declare_topology(channel)).exchange
        log.info("queue.connected", exchange=EXCHANGE, queue=QUEUE)

    async def stop(self) -> None:
        self._exchange = None
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def is_healthy(self) -> bool:
        """Used by `/readyz`; never raises. `reconnecting` is the part `is_closed`
        misses: aio-pika keeps a robust connection "open" while it retries, so without
        this check the api reports a healthy queue that cannot accept a publish."""
        connection = self._connection
        return (
            connection is not None
            and not connection.is_closed
            and not connection.reconnecting
            and self._exchange is not None
        )

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


class RabbitMqConsumer:
    """The worker's side of `claims.evaluate`.

    The ack decision is delegated to `should_requeue`, which lives in the worker
    interface: the broker adapter knows how to nack, not which failures deserve
    another go.

    A body that does not validate is dead-lettered without reaching the handler —
    there is no version of "retry" that makes malformed JSON parse.
    """

    def __init__(
        self,
        url: str,
        *,
        prefetch: int,
        handler: Callable[[EvaluationMessage], Awaitable[None]],
        should_requeue: Callable[[BaseException], bool],
        drain_timeout: float = 30.0,
    ) -> None:
        self._url = url
        self._prefetch = prefetch
        self._handler = handler
        self._should_requeue = should_requeue
        self._drain_timeout = drain_timeout
        self._connection: AbstractRobustConnection | None = None
        self._queue: AbstractQueue | None = None
        self._tag: str | None = None
        self._in_flight = 0
        self._idle = asyncio.Event()
        self._idle.set()

    async def start(self) -> None:
        self._connection = await connect_robust(self._url)
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=self._prefetch)
        self._queue = (await declare_topology(channel)).queue
        self._tag = await self._queue.consume(self._on_message)
        log.info("consumer.started", queue=QUEUE, prefetch=self._prefetch)

    async def stop(self) -> None:
        """Graceful: stop taking new deliveries, let the in-flight ones finish, close."""
        if self._queue is not None and self._tag is not None:
            with contextlib.suppress(Exception):
                await self._queue.cancel(self._tag)
            self._tag = None
        try:
            await asyncio.wait_for(self._idle.wait(), timeout=self._drain_timeout)
        except TimeoutError:
            log.warning("consumer.drain_timeout", in_flight=self._in_flight)
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
        self._queue = None
        log.info("consumer.stopped")

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        self._in_flight += 1
        self._idle.clear()
        try:
            try:
                parsed = EvaluationMessage.model_validate_json(message.body)
            except ValidationError as error:
                log.error(
                    "queue.undecodable",
                    message_id=message.message_id,
                    problems=error.error_count(),
                )
                await message.nack(requeue=False)
                return

            try:
                await self._handler(parsed)
            except BaseException as error:
                requeue = self._should_requeue(error)
                log.warning(
                    "queue.nacked",
                    claim_id=str(parsed.claim_id),
                    tenant_id=str(parsed.tenant_id),
                    requeue=requeue,
                    delivery_count=(message.headers or {}).get("x-delivery-count"),
                    error=type(error).__name__,
                )
                await message.nack(requeue=requeue)
            else:
                await message.ack()
        finally:
            self._in_flight -= 1
            if self._in_flight == 0:
                self._idle.set()
