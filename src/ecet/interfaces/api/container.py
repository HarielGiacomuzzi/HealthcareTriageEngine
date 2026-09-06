"""Wiring. Plain functions, no DI framework (per the api spec).

Everything expensive is built once here and lives for the process: the presidio
engine (2-4 s, ~600 MB), the SQLAlchemy engine, the AMQP connection. Routes reach it
through `request.app.state.container`.

The container's field types are the **ports**, not the adapters, so the API tests can
build one out of `tests/fakes.py` and exercise the real routes with no infrastructure.
"""

from collections.abc import Awaitable, Callable, MutableMapping
from dataclasses import dataclass, field

import structlog
from sqlalchemy import text

from ecet.application.ports.object_storage import ObjectStorage
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.redaction_policy import (
    CUSTOM_PATTERNS,
    ENTITY_REPLACEMENTS,
    SCORE_THRESHOLD,
)
from ecet.application.use_cases.enqueue_evaluation import EnqueueEvaluation
from ecet.application.use_cases.ingest_claim_document import IngestClaimDocument
from ecet.application.use_cases.redact_pii import RedactPii
from ecet.application.use_cases.run_deterministic_checks import RunDeterministicChecks
from ecet.config import Settings
from ecet.infrastructure.clock import SystemClock
from ecet.infrastructure.pdf.pypdf_extractor import PypdfTextExtractor
from ecet.infrastructure.pii.presidio_redactor import PresidioPiiRedactor
from ecet.infrastructure.postgres.migrations import assert_at_head, upgrade_to_head
from ecet.infrastructure.postgres.seed import load_seed
from ecet.infrastructure.postgres.session import create_engine, create_session_factory
from ecet.infrastructure.postgres.unit_of_work import SqlAlchemyUnitOfWork
from ecet.infrastructure.queue.rabbitmq import RabbitMqEvaluationQueue
from ecet.infrastructure.storage.s3 import S3ObjectStorage

log = structlog.get_logger(__name__)

Probe = Callable[[], Awaitable[bool]]


@dataclass
class ApiContainer:
    settings: Settings
    uow_factory: Callable[[], UnitOfWork]
    storage: ObjectStorage
    ingest: IngestClaimDocument
    #: `/readyz` checks. Mutable so a test can swap one out.
    probes: MutableMapping[str, Probe] = field(default_factory=dict)
    aclose: Callable[[], Awaitable[None]] = field(default_factory=lambda: _noop)


async def _noop() -> None:
    return None


async def build_container(settings: Settings) -> ApiContainer:
    engine = create_engine(settings.database_url.get_secret_value())

    if settings.auto_migrate:
        # Compose-only. Phase 2 shipped the migration and the seed but wired neither
        # into a process; this is where they run.
        await upgrade_to_head(settings.database_url.get_secret_value())
        if settings.env == "dev":
            statements = await load_seed(engine)
            log.info("db.seeded", statements=statements)
    else:
        await assert_at_head(engine)

    session_factory = create_session_factory(engine)
    queue = RabbitMqEvaluationQueue(settings.amqp_url.get_secret_value())
    await queue.start()

    redactor = PresidioPiiRedactor(
        replacements=ENTITY_REPLACEMENTS,
        custom_patterns=CUSTOM_PATTERNS,
        score_threshold=SCORE_THRESHOLD,
        spacy_model=settings.spacy_model,
        concurrency=settings.pii_concurrency,
    )
    storage = S3ObjectStorage(
        endpoint_url=settings.s3_endpoint,
        access_key=settings.s3_access_key.get_secret_value(),
        secret_key=settings.s3_secret_key.get_secret_value(),
    )
    clock = SystemClock()

    def uow_factory() -> UnitOfWork:
        return SqlAlchemyUnitOfWork(session_factory)

    ingest = IngestClaimDocument(
        uow_factory=uow_factory,
        storage=storage,
        extractor=PypdfTextExtractor(max_pages=settings.max_pdf_pages),
        redact_pii=RedactPii(redactor),
        run_checks=RunDeterministicChecks(),
        enqueue=EnqueueEvaluation(queue, clock),
        clock=clock,
        max_pdf_bytes=settings.max_pdf_bytes,
    )

    async def database_ready() -> bool:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        return True

    async def queue_ready() -> bool:
        return await queue.is_healthy()

    async def redactor_ready() -> bool:
        return True  # constructing it loaded the model; reaching here means it is up

    async def aclose() -> None:
        try:
            await queue.stop()
        finally:
            await engine.dispose()

    return ApiContainer(
        settings=settings,
        uow_factory=uow_factory,
        storage=storage,
        ingest=ingest,
        probes={
            "database": database_ready,
            "queue": queue_ready,
            "redactor": redactor_ready,
        },
        aclose=aclose,
    )
