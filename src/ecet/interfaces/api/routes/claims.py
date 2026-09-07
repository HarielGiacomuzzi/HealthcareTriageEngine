"""`/v1/claims` — manual ingest and the claim view.

`POST /v1/claims/ingest` exists for the demo and for local development: it takes
`{bucket, key}` and fills in the etag and size with a HEAD, so a developer can ingest
an object without forging an S3 notification.

`GET /v1/claims/{id}` is where tenant isolation is enforced by hand. `ClaimRepository`
has no tenant parameter, so the route compares the loaded claim's tenant against the
`X-Tenant-Id` header and answers 404 — never 403 — on a mismatch: a 403 would confirm
that the claim exists.
"""

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict

from ecet.application.use_cases.ingest_claim_document import IngestCommand, IngestResult
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import ClaimNotFound
from ecet.domain.evaluation import DeterministicResult, Evaluation
from ecet.domain.ids import ClaimId, PolicyId
from ecet.interfaces.api.dependencies import ContainerDep, TenantDep, require_api_key

router = APIRouter(tags=["claims"], dependencies=[Depends(require_api_key)])


class ManualIngestRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    bucket: str
    key: str


class ClaimView(BaseModel):
    """What a client may see. Deliberately excludes the redacted text: it is not a
    secret, but it is clinical content, and no caller in this phase needs it."""

    model_config = ConfigDict(frozen=True)

    id: UUID
    tenant_id: str
    status: ClaimStatus
    policy_ids: list[PolicyId]
    entity_counts: dict[str, int]
    deterministic: DeterministicResult | None
    evaluation: Evaluation | None
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def of(cls, claim: Claim) -> "ClaimView":
        return cls(
            id=claim.id,
            tenant_id=str(claim.tenant_id),
            status=claim.status,
            policy_ids=list(claim.policy_ids),
            entity_counts=dict(claim.redacted.entity_counts) if claim.redacted else {},
            deterministic=claim.deterministic,
            evaluation=claim.evaluation,
            failure_reason=claim.failure_reason,
            created_at=claim.created_at,
            updated_at=claim.updated_at,
        )


@router.post("/v1/claims/ingest")
async def ingest_claim(body: ManualIngestRequest, container: ContainerDep) -> IngestResult:
    head = await container.storage.head(body.bucket, body.key)
    return await container.ingest.execute(
        IngestCommand(bucket=body.bucket, key=body.key, etag=head.etag, size=head.size)
    )


@router.get("/v1/claims/{claim_id}")
async def get_claim(claim_id: UUID, tenant_id: TenantDep, container: ContainerDep) -> ClaimView:
    async with container.uow_factory() as uow:
        claim = await uow.claims.get(ClaimId(claim_id))
    if claim.tenant_id != tenant_id:
        # Same answer as "no such claim" — anything else confirms its existence.
        raise ClaimNotFound(str(claim_id))
    return ClaimView.of(claim)
