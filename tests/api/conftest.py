"""API tests run against the real app with fake ports.

`create_app(settings, container=...)` skips the lifespan build, so no Postgres, no
RabbitMQ, no spaCy model — the routes, the auth and the error mapping are what is
under test here.
"""

from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

import httpx
import pytest
from tests.fakes import (
    FakeEvaluationQueue,
    FakeObjectStorage,
    FakePiiRedactor,
    FakeTextExtractor,
    FakeUnitOfWork,
    FixedClock,
)
from tests.pii import read_note

from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.use_cases.enqueue_evaluation import EnqueueEvaluation
from ecet.application.use_cases.ingest_claim_document import IngestClaimDocument
from ecet.application.use_cases.redact_pii import RedactPii
from ecet.application.use_cases.run_deterministic_checks import RunDeterministicChecks
from ecet.config import Settings
from ecet.domain.ids import PolicyId
from ecet.domain.policy import Policy
from ecet.domain.tenant import Tenant
from ecet.interfaces.api.app import create_app
from ecet.interfaces.api.container import ApiContainer

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
BUCKET = "claims"
KEY = "tenants/tenant-a/claims/note-1.pdf"
PDF = b"%PDF-1.7 fake bytes"
API_KEY = "test-key"
EVENT_TOKEN = "test-token"


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": "dev-hmac-tenant-a",
        }
    )


def build_policy() -> Policy:
    return Policy.model_validate(
        {
            "id": PolicyId(uuid4()),
            "tenant_id": "tenant-a",
            "name": "MRI lumbar spine",
            "version": 2,
            "covered_codes": [{"code": "M54.5"}],
            "excluded_codes": [{"code": "Z00.00"}],
            "criteria_text": "Imaging is covered after six weeks of conservative therapy.",
            "effective_from": date(2026, 1, 1),
        }
    )


class ApiHarness:
    def __init__(self, *, note: str = "meets", tenants: list[Tenant] | None = None) -> None:
        self.clock = FixedClock(NOW)
        self.uow = FakeUnitOfWork(
            tenants=tenants if tenants is not None else [build_tenant()],
            policies=[build_policy()],
        )
        self.storage = FakeObjectStorage({(BUCKET, KEY): PDF})
        self.queue = FakeEvaluationQueue()
        self.settings = Settings(_env_file=None, s3_event_token=EVENT_TOKEN, api_key=API_KEY)
        uow_factory: Callable[[], UnitOfWork] = self._uow_factory
        self.ingest = IngestClaimDocument(
            uow_factory=uow_factory,
            storage=self.storage,
            extractor=FakeTextExtractor(read_note(note)),
            redact_pii=RedactPii(FakePiiRedactor()),
            run_checks=RunDeterministicChecks(),
            enqueue=EnqueueEvaluation(self.queue, self.clock),
            clock=self.clock,
            max_pdf_bytes=self.settings.max_pdf_bytes,
        )
        self.probe_results: dict[str, bool] = {
            "database": True,
            "queue": True,
            "redactor": True,
        }
        self.container = ApiContainer(
            settings=self.settings,
            uow_factory=uow_factory,
            storage=self.storage,
            ingest=self.ingest,
            probes={name: self._probe(name) for name in self.probe_results},
            aclose=self._aclose,
        )
        self.app = create_app(self.settings, container=self.container)

    def _uow_factory(self) -> UnitOfWork:
        return self.uow

    def _probe(self, name: str) -> Callable[[], Any]:
        async def check() -> bool:
            return self.probe_results[name]

        return check

    async def _aclose(self) -> None:
        return None

    def client(self, **kwargs: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://test", **kwargs
        )


@pytest.fixture
def harness() -> ApiHarness:
    return ApiHarness()


@pytest.fixture
def api_headers() -> dict[str, str]:
    return {"X-API-Key": API_KEY, "X-Tenant-Id": "tenant-a"}


@pytest.fixture
def event_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {EVENT_TOKEN}"}
