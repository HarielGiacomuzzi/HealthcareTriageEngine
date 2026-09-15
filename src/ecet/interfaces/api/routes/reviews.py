"""`/v1/reviews` — the human-review queue (UC-09b) and its decisions (UC-09c).

Both routes are tenant-scoped by `X-Tenant-Id`, and a task belonging to another tenant
answers 404 exactly like one that does not exist.

`GET /v1/reviews` is the one API response that carries claim text, and it is the
redacted text: a reviewer cannot decide without reading the note.

`POST /v1/reviews/{id}/resolve` answers 200 once the decision is recorded, even when
its webhook then fails. The reviewer's work is done, and `claim_status: NOTIFY_FAILED`
in the body is the cue for `POST /v1/claims/{id}/retry-notify`. The delivery — up to
`ECET_WEBHOOK_MAX_ATTEMPTS` attempts with backoff — runs inside the request.
"""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field

from ecet.application.use_cases.human_review import (
    ResolveReviewCommand,
    ResolveReviewResult,
    ReviewTaskView,
)
from ecet.domain.evaluation import HumanResolution
from ecet.interfaces.api.dependencies import ContainerDep, TenantDep, require_api_key

router = APIRouter(tags=["reviews"], dependencies=[Depends(require_api_key)])


class ResolveReviewRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    reviewer: str = Field(min_length=1, max_length=200)
    resolution: HumanResolution
    notes: str | None = Field(default=None, max_length=2000)


@router.get("/v1/reviews")
async def list_reviews(
    tenant_id: TenantDep,
    container: ContainerDep,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> list[ReviewTaskView]:
    return await container.list_reviews.execute(tenant_id, limit)


@router.post("/v1/reviews/{task_id}/resolve")
async def resolve_review(
    task_id: UUID,
    body: ResolveReviewRequest,
    tenant_id: TenantDep,
    container: ContainerDep,
) -> ResolveReviewResult:
    return await container.resolve_review.execute(
        ResolveReviewCommand(
            task_id=task_id,
            tenant_id=tenant_id,
            reviewer=body.reviewer,
            resolution=body.resolution,
            notes=body.notes,
        )
    )
