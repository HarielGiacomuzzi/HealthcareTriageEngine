"""The consumer against a real broker. The three outcomes that matter are the ones
the DLQ makes visible: acked and gone, requeued and redelivered, dead-lettered."""

import asyncio
from collections.abc import AsyncIterator, Callable, Iterator

import aio_pika
import pytest
from testcontainers.core.container import DockerContainer
from tests.adapters.containers import wait_until
from tests.adapters.test_rabbitmq_queue import build_message
from tests.pii import assert_no_pii

from ecet.application.errors import LLMTransientError
from ecet.application.messages import EvaluationMessage
from ecet.infrastructure.queue.rabbitmq import (
    DLQ,
    DLX,
    QUEUE,
    ROUTING_KEY,
    RabbitMqConsumer,
    RabbitMqEvaluationQueue,
    replay_dead_letters,
)
from ecet.interfaces.worker.handler import should_requeue


@pytest.fixture(scope="session")
def amqp_url() -> Iterator[str]:
    container = DockerContainer("rabbitmq:3.13-management").with_exposed_ports(5672)
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        yield f"amqp://guest:guest@{host}:{port}/"


@pytest.fixture
async def publisher(amqp_url: str) -> AsyncIterator[RabbitMqEvaluationQueue]:
    adapter = RabbitMqEvaluationQueue(amqp_url)
    await wait_until(adapter.start)
    yield adapter
    await adapter.stop()


async def drain(
    predicate: Callable[[], bool],
    timeout: float = 15.0,  # noqa: ASYNC109 -- a polling deadline, not a single call to wrap
) -> None:
    """Poll until `predicate()` is true or the deadline passes."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError("condition not reached before the deadline")


async def test_a_published_message_reaches_the_handler(
    publisher: RabbitMqEvaluationQueue, amqp_url: str
) -> None:
    handled: list[EvaluationMessage] = []

    async def handle(message: EvaluationMessage) -> None:
        handled.append(message)

    consumer = RabbitMqConsumer(amqp_url, prefetch=4, handler=handle, should_requeue=should_requeue)
    await consumer.start()
    try:
        sent = build_message()
        await publisher.publish(sent)
        await drain(lambda: len(handled) == 1)
    finally:
        await consumer.stop()

    assert handled[0].claim_id == sent.claim_id
    assert_no_pii(handled[0].model_dump_json())


async def test_a_dlq_classified_failure_dead_letters_the_message(
    publisher: RabbitMqEvaluationQueue, amqp_url: str
) -> None:
    attempts: list[int] = []

    async def handle(message: EvaluationMessage) -> None:
        attempts.append(1)
        raise ValueError("a bug, not a blip")

    consumer = RabbitMqConsumer(amqp_url, prefetch=4, handler=handle, should_requeue=should_requeue)
    await consumer.start()
    try:
        await publisher.publish(build_message())
        await drain(lambda: len(attempts) >= 1)
        await asyncio.sleep(1.0)  # let the broker route it to the DLX
    finally:
        await consumer.stop()

    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        dead_letters = await channel.get_queue(DLQ)
        dead = await dead_letters.get(timeout=10)
        assert dead is not None
        await dead.ack()

    assert len(attempts) == 1
    assert_no_pii(dead.body.decode("utf-8"))


async def test_an_undecodable_body_dead_letters_without_reaching_the_handler(
    amqp_url: str,
) -> None:
    handled: list[EvaluationMessage] = []

    async def handle(message: EvaluationMessage) -> None:
        handled.append(message)

    consumer = RabbitMqConsumer(amqp_url, prefetch=4, handler=handle, should_requeue=should_requeue)
    await consumer.start()
    try:
        connection = await aio_pika.connect_robust(amqp_url)
        async with connection:
            channel = await connection.channel()
            exchange = await channel.get_exchange("ecet")
            await exchange.publish(
                aio_pika.Message(body=b"{not json}"), routing_key="claims.evaluate"
            )
        await asyncio.sleep(2.0)
    finally:
        await consumer.stop()

    assert handled == []

    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        dead_letters = await channel.get_queue(DLQ)
        dead = await dead_letters.get(timeout=10)
        assert dead is not None
        await dead.ack()

    assert_no_pii(dead.body.decode("utf-8"))


async def test_a_requeue_classified_failure_is_redelivered(
    publisher: RabbitMqEvaluationQueue, amqp_url: str
) -> None:
    attempts: list[int] = []

    async def handle(message: EvaluationMessage) -> None:
        attempts.append(1)
        if len(attempts) < 2:
            raise LLMTransientError("429")

    consumer = RabbitMqConsumer(amqp_url, prefetch=4, handler=handle, should_requeue=should_requeue)
    await consumer.start()
    try:
        await publisher.publish(build_message())
        await drain(lambda: len(attempts) >= 2, timeout=20.0)
    finally:
        await consumer.stop()

    assert len(attempts) >= 2


async def test_dlq_replay_moves_dead_letters_back_within_the_limit(
    publisher: RabbitMqEvaluationQueue, amqp_url: str
) -> None:
    # `publisher` has declared the topology. Start from empty queues so leftovers from
    # the other tests in this module cannot be mistaken for a replayed message.
    sent = [build_message(), build_message()]
    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        await (await channel.get_queue(QUEUE)).purge()
        await (await channel.get_queue(DLQ)).purge()
        dlx = await channel.get_exchange(DLX)
        for message in sent:
            await dlx.publish(
                aio_pika.Message(
                    body=message.model_dump_json().encode("utf-8"),
                    content_type="application/json",
                    message_id=str(message.message_id),
                    headers={"x-tenant-id": "tenant-a", "x-schema-version": 1},
                ),
                routing_key=ROUTING_KEY,
            )

    assert await replay_dead_letters(amqp_url, limit=1) == 1
    assert await replay_dead_letters(amqp_url, limit=10) == 1
    assert await replay_dead_letters(amqp_url, limit=10) == 0

    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        work = await channel.get_queue(QUEUE)
        replayed = []
        for _ in sent:
            delivered = await work.get(timeout=10)
            assert delivered is not None
            await delivered.ack()
            replayed.append(delivered)
        assert await (await channel.get_queue(DLQ)).get(fail=False) is None

    claim_ids = {EvaluationMessage.model_validate_json(m.body).claim_id for m in replayed}
    assert claim_ids == {message.claim_id for message in sent}
    assert all(m.headers["x-tenant-id"] == "tenant-a" for m in replayed)
    assert all(m.delivery_mode == 2 for m in replayed)
    for m in replayed:
        assert_no_pii(m.body.decode("utf-8"))
