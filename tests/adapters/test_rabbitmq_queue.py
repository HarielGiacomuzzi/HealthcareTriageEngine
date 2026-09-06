import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from uuid import uuid4

import aio_pika
import pytest
from testcontainers.core.container import DockerContainer
from tests.adapters.containers import wait_until

from ecet.application.errors import QueuePublishError
from ecet.application.messages import EvaluationMessage, PolicySnapshot
from ecet.domain.ids import ClaimId, PolicyId, TenantId
from ecet.infrastructure.queue.rabbitmq import QUEUE, RabbitMqEvaluationQueue

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


def build_message() -> EvaluationMessage:
    return EvaluationMessage(
        message_id=uuid4(),
        claim_id=ClaimId(uuid4()),
        tenant_id=TenantId("tenant-a"),
        redacted_text="Patient <PERSON> with M54.5.",
        policies=[
            PolicySnapshot(
                id=PolicyId(uuid4()),
                name="MRI lumbar spine",
                version=2,
                covered_codes=["M54.5"],
                excluded_codes=[],
                criteria_text="Covered after six weeks of conservative therapy.",
                required_evidence=[],
            )
        ],
        deterministic_verdict="PASS",
        found_codes=["M54.5"],
        enqueued_at=NOW,
    )


@pytest.fixture(scope="session")
def amqp_url() -> Iterator[str]:
    container = DockerContainer("rabbitmq:3.13-management").with_exposed_ports(5672)
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        yield f"amqp://guest:guest@{host}:{port}/"


@pytest.fixture
async def queue(amqp_url: str) -> AsyncIterator[RabbitMqEvaluationQueue]:
    adapter = RabbitMqEvaluationQueue(amqp_url)
    # RabbitMQ opens 5672 well before it accepts AMQP handshakes.
    await wait_until(adapter.start)
    yield adapter
    await adapter.stop()


async def test_a_published_message_lands_on_the_queue(
    queue: RabbitMqEvaluationQueue, amqp_url: str
) -> None:
    message = build_message()

    await queue.publish(message)

    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        declared = await channel.get_queue(QUEUE)
        delivered = await declared.get(timeout=10)
        assert delivered is not None
        await delivered.ack()

    assert delivered.content_type == "application/json"
    assert delivered.message_id == str(message.message_id)
    assert delivered.headers["x-tenant-id"] == "tenant-a"
    assert delivered.headers["x-schema-version"] == 1
    assert delivered.delivery_mode == 2
    assert json.loads(delivered.body)["claim_id"] == str(message.claim_id)


async def test_the_topology_is_declared_idempotently(
    queue: RabbitMqEvaluationQueue, amqp_url: str
) -> None:
    # A second start against an existing topology must not raise (the api and the
    # worker both declare it).
    second = RabbitMqEvaluationQueue(amqp_url)
    await second.start()
    await second.stop()


async def test_publishing_after_stop_raises_queue_publish_error(
    queue: RabbitMqEvaluationQueue,
) -> None:
    await queue.stop()

    with pytest.raises(QueuePublishError):
        await queue.publish(build_message())


def test_the_adapter_satisfies_the_port(amqp_url: str) -> None:
    from ecet.application.ports.evaluation_queue import EvaluationQueue

    assert isinstance(RabbitMqEvaluationQueue(amqp_url), EvaluationQueue)
