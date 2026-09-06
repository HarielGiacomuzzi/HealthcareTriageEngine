"""In-memory port implementations for tests.

Phase 1 ships the four domain repository fakes. The application-port fakes
(`FakeObjectStorage`, `FakePiiRedactor`, `FakeLLMGateway`, …) arrive with the phases
that define those ports.
"""

from collections.abc import Iterable
from datetime import date
from types import TracebackType
from uuid import UUID

from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import ClaimNotFound, ReviewTaskNotFound, TenantNotFound
from ecet.domain.evaluation import ReviewStatus, ReviewTask
from ecet.domain.ids import ClaimId, PolicyId, TenantId
from ecet.domain.policy import Icd10Code, Policy
from ecet.domain.tenant import Tenant


class FakeClaimRepository:
    def __init__(self) -> None:
        self.claims: dict[ClaimId, Claim] = {}
        self.saved: list[ClaimId] = []

    async def add(self, claim: Claim) -> None:
        self.claims[claim.id] = claim

    async def get(self, claim_id: ClaimId) -> Claim:
        try:
            return self.claims[claim_id]
        except KeyError:
            raise ClaimNotFound(str(claim_id)) from None

    async def find_by_source(self, bucket: str, key: str, etag: str) -> Claim | None:
        for claim in self.claims.values():
            source = claim.source
            if (source.bucket, source.key, source.etag) == (bucket, key, etag):
                return claim
        return None

    async def save(self, claim: Claim) -> None:
        self.claims[claim.id] = claim
        self.saved.append(claim.id)

    async def list_by_status(self, status: ClaimStatus, *, limit: int = 50) -> list[Claim]:
        return [claim for claim in self.claims.values() if claim.status is status][:limit]


class FakePolicyRepository:
    def __init__(self, policies: Iterable[Policy] = ()) -> None:
        self.policies: list[Policy] = list(policies)

    async def active_for_tenant(self, tenant_id: TenantId, *, on: date) -> list[Policy]:
        matching = [
            policy
            for policy in self.policies
            if policy.tenant_id == tenant_id and policy.is_effective(on)
        ]
        highest: dict[str, Policy] = {}
        for policy in matching:
            current = highest.get(policy.name)
            if current is None or policy.version > current.version:
                highest[policy.name] = policy
        return list(highest.values())

    async def get_many(self, ids: Iterable[PolicyId]) -> list[Policy]:
        wanted = set(ids)
        return [policy for policy in self.policies if policy.id in wanted]


class FakeTenantRepository:
    def __init__(self, tenants: Iterable[Tenant] = ()) -> None:
        self.tenants: dict[str, Tenant] = {tenant.id: tenant for tenant in tenants}

    async def get(self, tenant_id: TenantId) -> Tenant:
        tenant = self.tenants.get(tenant_id)
        if tenant is None or not tenant.active:
            raise TenantNotFound(tenant_id)
        return tenant


class FakeReviewTaskRepository:
    def __init__(self) -> None:
        self.tasks: dict[UUID, ReviewTask] = {}

    async def add(self, task: ReviewTask) -> None:
        self.tasks[task.id] = task

    async def get(self, task_id: UUID) -> ReviewTask:
        try:
            return self.tasks[task_id]
        except KeyError:
            raise ReviewTaskNotFound(str(task_id)) from None

    async def find_open_by_claim(self, claim_id: ClaimId) -> ReviewTask | None:
        return next(
            (
                task
                for task in self.tasks.values()
                if task.claim_id == claim_id and task.status is ReviewStatus.OPEN
            ),
            None,
        )

    async def list_open(self, tenant_id: TenantId, limit: int = 50) -> list[ReviewTask]:
        return [
            task
            for task in self.tasks.values()
            if task.tenant_id == tenant_id and task.status is ReviewStatus.OPEN
        ][:limit]

    async def save(self, task: ReviewTask) -> None:
        self.tasks[task.id] = task


class FakeIcd10CodeRepository:
    def __init__(self, codes: Iterable[Icd10Code] = ()) -> None:
        self.codes = frozenset(codes)

    async def known_codes(self) -> frozenset[Icd10Code]:
        return self.codes


class FakeUnitOfWork:
    """In-memory unit of work. `commit()` records the call; the fakes never roll back,
    because nothing they hold is transactional."""

    def __init__(
        self,
        *,
        tenants: Iterable[Tenant] = (),
        policies: Iterable[Policy] = (),
        known_codes: Iterable[Icd10Code] = (),
    ) -> None:
        self.claims = FakeClaimRepository()
        self.policies = FakePolicyRepository(policies)
        self.tenants = FakeTenantRepository(tenants)
        self.review_tasks = FakeReviewTaskRepository()
        self.icd10_codes = FakeIcd10CodeRepository(known_codes)
        self.commits = 0

    async def __aenter__(self) -> "FakeUnitOfWork":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    async def commit(self) -> None:
        self.commits += 1
