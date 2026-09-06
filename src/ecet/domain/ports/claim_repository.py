"""Claim persistence contract. Implemented by Postgres (Phase 2) and by test fakes."""

from typing import Protocol, runtime_checkable

from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.ids import ClaimId


@runtime_checkable
class ClaimRepository(Protocol):
    async def add(self, claim: Claim) -> None: ...

    async def get(self, claim_id: ClaimId) -> Claim:
        """Raises `ClaimNotFound`."""
        ...

    async def find_by_source(self, bucket: str, key: str, etag: str) -> Claim | None:
        """ADR-006 idempotency lookup."""
        ...

    async def save(self, claim: Claim) -> None:
        """Full overwrite, optimistic on `updated_at`. Raises `ConcurrentModification`."""
        ...

    async def list_by_status(self, status: ClaimStatus, *, limit: int = 50) -> list[Claim]: ...
