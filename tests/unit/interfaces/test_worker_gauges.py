"""The gauges are the only metric nobody's request path produces, so the worker goes
and asks. A failed refresh must not take the worker down with it — a wrong gauge is
cheaper than a dead consumer."""

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

from prometheus_client import REGISTRY
from tests.fakes import FakeUnitOfWork

from ecet.domain.claim import Claim, ClaimStatus, SourceObject
from ecet.domain.evaluation import ReviewReason, ReviewTask
from ecet.domain.ids import ClaimId, TenantId
from ecet.interfaces.worker.gauges import refresh_once, run_refresher

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)


def build_claim(status: ClaimStatus = ClaimStatus.RECEIVED, tenant_id: str = "tenant-a") -> Claim:
    return Claim(
        id=ClaimId(uuid4()),
        tenant_id=tenant_id,
        source=SourceObject(
            bucket="claims", key=f"tenants/{tenant_id}/claims/a.pdf", etag="etag-1", size=10
        ),
        status=status,
        created_at=NOW,
        updated_at=NOW,
    )


def build_task(tenant_id: TenantId) -> ReviewTask:
    return ReviewTask(
        id=uuid4(),
        claim_id=ClaimId(uuid4()),
        tenant_id=tenant_id,
        reason=ReviewReason.LOW_CONFIDENCE,
        created_at=NOW,
    )


def sample(name: str, **labels: str) -> float:
    value = REGISTRY.get_sample_value(name, labels)
    return 0.0 if value is None else value


async def test_refresh_sets_a_gauge_per_status_and_tenant() -> None:
    uow = FakeUnitOfWork()
    await uow.claims.add(build_claim(status=ClaimStatus.QUEUED))
    await uow.review_tasks.add(build_task(tenant_id=TenantId("tenant-a")))

    await refresh_once(lambda: uow)

    assert sample("ecet_claims_by_status", status="QUEUED") == 1
    assert sample("ecet_claims_by_status", status="APPROVED_AUTO") == 0
    assert sample("ecet_review_open", tenant="tenant-a") == 1


async def test_a_refresh_failure_does_not_stop_the_loop() -> None:
    calls = 0

    def failing_factory() -> FakeUnitOfWork:
        nonlocal calls
        calls += 1
        raise ConnectionError("database is away")

    stop = asyncio.Event()
    task = asyncio.create_task(run_refresher(failing_factory, stop, interval=0.01))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert calls > 1  # it kept going after the first failure


async def test_the_refresher_returns_when_stop_is_set() -> None:
    uow = FakeUnitOfWork()
    stop = asyncio.Event()
    task = asyncio.create_task(run_refresher(lambda: uow, stop, interval=30.0))
    await asyncio.sleep(0)
    stop.set()

    # It must wake on the event, not sleep out the 30 s interval.
    await asyncio.wait_for(task, timeout=1)
