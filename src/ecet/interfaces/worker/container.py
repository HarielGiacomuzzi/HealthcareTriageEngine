"""Worker wiring. Same shape as the api container: ports in the fields, adapters built
once, everything closed in `aclose`.

The worker never migrates. `ECET_AUTO_MIGRATE` is an api-only setting (see the config
spec), and two processes racing `alembic upgrade head` against one database is a
worse failure than starting a moment later — compose gates the worker on the api being
healthy, which is exactly "migrations have run".
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import structlog

from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.use_cases.evaluate_claim import EvaluateClaim
from ecet.config import Settings
from ecet.infrastructure.clock import SystemClock
from ecet.infrastructure.llm.factory import build_gateway
from ecet.infrastructure.postgres.migrations import assert_at_head
from ecet.infrastructure.postgres.session import create_engine, create_session_factory
from ecet.infrastructure.postgres.unit_of_work import SqlAlchemyUnitOfWork
from ecet.infrastructure.queue.rabbitmq import RabbitMqConsumer
from ecet.infrastructure.webhook.httpx_client import HttpxWebhookClient
from ecet.interfaces.worker.handler import WorkerMessageHandler, should_requeue

log = structlog.get_logger(__name__)


@dataclass
class WorkerContainer:
    settings: Settings
    evaluate: EvaluateClaim
    consumer: RabbitMqConsumer
    aclose: Callable[[], Awaitable[None]]


async def build_container(settings: Settings) -> WorkerContainer:
    engine = create_engine(settings.database_url.get_secret_value())
    await assert_at_head(engine)
    session_factory = create_session_factory(engine)

    llm = build_gateway(settings)
    webhook = HttpxWebhookClient(
        timeout_s=settings.webhook_timeout_s, max_attempts=settings.webhook_max_attempts
    )

    def uow_factory() -> UnitOfWork:
        # One per message: `SqlAlchemyUnitOfWork` does not reopen its session.
        return SqlAlchemyUnitOfWork(session_factory)

    evaluate = EvaluateClaim(
        uow_factory=uow_factory,
        llm=llm,
        webhook=webhook,
        clock=SystemClock(),
        threshold=settings.confidence_threshold,
        prompt_version=settings.prompt_version,
    )
    consumer = RabbitMqConsumer(
        settings.amqp_url.get_secret_value(),
        prefetch=settings.worker_prefetch,
        handler=WorkerMessageHandler(evaluate).handle,
        should_requeue=should_requeue,
    )

    async def aclose() -> None:
        try:
            await consumer.stop()
        finally:
            try:
                await webhook.aclose()
            finally:
                await engine.dispose()

    log.info(
        "worker.container_built",
        provider=settings.llm_provider.value,
        model=settings.llm_model,
        threshold=settings.confidence_threshold,
        prefetch=settings.worker_prefetch,
    )
    return WorkerContainer(settings=settings, evaluate=evaluate, consumer=consumer, aclose=aclose)
