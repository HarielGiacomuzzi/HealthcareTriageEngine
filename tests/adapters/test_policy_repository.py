from datetime import date
from typing import Any
from uuid import uuid4

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tests.adapters.helpers import build_tenant, insert_policy, insert_tenant

from ecet.domain.ids import PolicyId, TenantId
from ecet.domain.policy import Icd10Code, Policy
from ecet.infrastructure.postgres.orm import Icd10CodeRow
from ecet.infrastructure.postgres.repositories import (
    PostgresIcd10CodeRepository,
    PostgresPolicyRepository,
)

ON = date(2026, 6, 1)


def build_policy(**overrides: Any) -> Policy:
    fields: dict[str, Any] = {
        "id": PolicyId(uuid4()),
        "tenant_id": "tenant-a",
        "name": "MRI lumbar spine",
        "version": 1,
        "covered_codes": {Icd10Code(code="M54.5")},
        "excluded_codes": {Icd10Code(code="Z00.00")},
        "criteria_text": "Conservative therapy for at least six weeks.",
        "required_evidence": ["imaging report"],
        "effective_from": date(2026, 1, 1),
    }
    fields.update(overrides)
    return Policy.model_validate(fields)


async def _seed(session: AsyncSession, *policies: Policy) -> None:
    for tenant_id in {policy.tenant_id for policy in policies}:
        await insert_tenant(session, build_tenant(str(tenant_id)))
    for policy in policies:
        await insert_policy(session, policy)
    await session.commit()


async def test_only_the_highest_version_of_a_name_is_returned(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    v1 = build_policy(version=1)
    v2 = build_policy(version=2, covered_codes={Icd10Code(code="M54.4")})
    async with session_factory() as session:
        await _seed(session, v1, v2)
        found = await PostgresPolicyRepository(session).active_for_tenant(
            TenantId("tenant-a"), on=ON
        )
    assert found == [v2]


async def test_inactive_expired_and_future_policies_are_filtered_out(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    current = build_policy(name="MRI lumbar spine")
    inactive = build_policy(name="Knee arthroscopy", active=False)
    expired = build_policy(name="Genetic panel", effective_to=date(2026, 3, 31))
    future = build_policy(name="Insulin pump", effective_from=date(2026, 9, 1))
    async with session_factory() as session:
        await _seed(session, current, inactive, expired, future)
        found = await PostgresPolicyRepository(session).active_for_tenant(
            TenantId("tenant-a"), on=ON
        )
    assert found == [current]


async def test_the_effective_window_is_inclusive_on_both_ends(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    policy = build_policy(effective_from=date(2026, 6, 1), effective_to=date(2026, 6, 1))
    async with session_factory() as session:
        await _seed(session, policy)
        repository = PostgresPolicyRepository(session)
        assert await repository.active_for_tenant(TenantId("tenant-a"), on=ON) == [policy]
        assert await repository.active_for_tenant(TenantId("tenant-a"), on=date(2026, 6, 2)) == []


async def test_policies_are_scoped_to_their_tenant(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    mine = build_policy(tenant_id="tenant-a")
    theirs = build_policy(tenant_id="tenant-b")
    async with session_factory() as session:
        await _seed(session, mine, theirs)
        found = await PostgresPolicyRepository(session).active_for_tenant(
            TenantId("tenant-a"), on=ON
        )
    assert found == [mine]


async def test_a_tenant_with_no_policies_gets_an_empty_list(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await insert_tenant(session, build_tenant("tenant-empty"))
        await session.commit()
        found = await PostgresPolicyRepository(session).active_for_tenant(
            TenantId("tenant-empty"), on=ON
        )
    assert found == []


async def test_get_many_returns_only_the_requested_ids(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    wanted = build_policy(name="MRI lumbar spine")
    other = build_policy(name="Sleep study")
    async with session_factory() as session:
        await _seed(session, wanted, other)
        repository = PostgresPolicyRepository(session)
        assert await repository.get_many([wanted.id]) == [wanted]
        assert await repository.get_many([]) == []


async def test_the_icd10_catalogue_is_read_as_a_set_of_codes(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await session.execute(
            insert(Icd10CodeRow),
            [
                {"code": "M54.5", "description": "Low back pain"},
                {"code": "G47.33", "description": "Obstructive sleep apnea"},
            ],
        )
        await session.commit()
        codes = await PostgresIcd10CodeRepository(session).known_codes()
    assert codes == {Icd10Code(code="M54.5"), Icd10Code(code="G47.33")}
