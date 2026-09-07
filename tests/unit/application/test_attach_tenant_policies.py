from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

import pytest
from tests.fakes import FakePolicyRepository, FixedClock

from ecet.application.use_cases.attach_tenant_policies import AttachTenantPolicies
from ecet.domain.errors import NoPoliciesForTenant
from ecet.domain.ids import PolicyId, TenantId
from ecet.domain.policy import Policy

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


def build_policy(name: str, **overrides: Any) -> Policy:
    fields: dict[str, Any] = {
        "id": PolicyId(uuid4()),
        "tenant_id": "tenant-a",
        "name": name,
        "version": 1,
        "covered_codes": [{"code": "M54.5"}],
        "criteria_text": "Covered after six weeks of conservative therapy.",
        "effective_from": date(2025, 1, 1),
    }
    fields.update(overrides)
    return Policy.model_validate(fields)


async def test_it_returns_only_the_effective_policies() -> None:
    repository = FakePolicyRepository(
        [
            build_policy("MRI lumbar spine"),
            build_policy("Polysomnography"),
            build_policy("Knee arthroscopy", active=False),
            build_policy("Gene panel", effective_to=date(2026, 3, 31)),
        ]
    )
    use_case = AttachTenantPolicies(repository, FixedClock(NOW))

    found = await use_case.execute(TenantId("tenant-a"))

    assert sorted(policy.name for policy in found) == ["MRI lumbar spine", "Polysomnography"]


async def test_a_tenant_with_no_policies_is_a_hard_failure() -> None:
    use_case = AttachTenantPolicies(FakePolicyRepository(), FixedClock(NOW))

    with pytest.raises(NoPoliciesForTenant, match="tenant-empty"):
        await use_case.execute(TenantId("tenant-empty"))


async def test_it_asks_the_repository_for_todays_date() -> None:
    repository = FakePolicyRepository([build_policy("MRI lumbar spine")])
    use_case = AttachTenantPolicies(repository, FixedClock(NOW))

    # A policy that starts tomorrow must not come back today.
    repository.policies.append(build_policy("Future policy", effective_from=date(2026, 9, 7)))
    found = await use_case.execute(TenantId("tenant-a"))

    assert [policy.name for policy in found] == ["MRI lumbar spine"]
