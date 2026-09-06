"""Shared builders and raw inserts for the adapter tests."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from pydantic import SecretStr
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ecet.domain.claim import Claim, SourceObject
from ecet.domain.ids import ClaimId, TenantId
from ecet.domain.policy import Policy
from ecet.domain.tenant import Tenant
from ecet.infrastructure.postgres.mappers import (
    claim_to_row_values,
    policy_to_row_values,
    tenant_to_row_values,
)
from ecet.infrastructure.postgres.orm import ClaimRow, PolicyRow, TenantRow

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
LATER = NOW + timedelta(minutes=5)


def build_tenant(tenant_id: str, **overrides: Any) -> Tenant:
    fields: dict[str, Any] = {
        "id": TenantId(tenant_id),
        "name": tenant_id,
        "webhook_url": f"http://mock-client:8081/hooks/{tenant_id}",
        "webhook_secret": SecretStr(f"dev-hmac-{tenant_id}"),
    }
    fields.update(overrides)
    return Tenant.model_validate(fields)


def build_claim(**overrides: Any) -> Claim:
    """A `RECEIVED` claim for `tenant-a`; `suffix` keeps keys and etags unique."""
    suffix = overrides.pop("suffix", "a")
    tenant_id = overrides.get("tenant_id", "tenant-a")
    fields: dict[str, Any] = {
        "id": ClaimId(uuid4()),
        "tenant_id": tenant_id,
        "source": SourceObject(
            bucket="claims",
            key=f"tenants/{tenant_id}/claims/note-{suffix}.pdf",
            etag=f"etag-{suffix}",
            size=12_345,
        ),
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return Claim.model_validate(fields)


async def insert_tenant(session: AsyncSession, tenant: Tenant) -> None:
    await session.execute(insert(TenantRow).values(**tenant_to_row_values(tenant)))


async def insert_policy(session: AsyncSession, policy: Policy) -> None:
    await session.execute(insert(PolicyRow).values(**policy_to_row_values(policy)))


async def insert_claim(session: AsyncSession, claim: Claim) -> None:
    await session.execute(insert(ClaimRow).values(**claim_to_row_values(claim)))
