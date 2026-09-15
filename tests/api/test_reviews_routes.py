"""`/v1/reviews`. UC-09b and UC-09c are covered with fakes in `tests/unit/application`;
this file pins the HTTP contract — the status codes, the tenant header, and that the one
route returning note text returns only redacted text."""

from uuid import uuid4

from tests.api.conftest import API_KEY, BUCKET, NOW, ApiHarness
from tests.fakes import FakePiiRedactor
from tests.pii import assert_no_pii, read_note

from ecet.application.errors import WebhookTransientError
from ecet.domain.claim import Claim, ClaimStatus, SourceObject
from ecet.domain.evaluation import ReviewReason, ReviewStatus, ReviewTask
from ecet.domain.ids import ClaimId

RESOLUTION = {
    "reviewer": "nurse.okafor",
    "resolution": "MEETS_NECESSITY",
    "notes": "Therapy dates confirmed with the provider's office.",
}


async def seed_review(harness: ApiHarness, *, tenant_id: str = "tenant-a") -> ReviewTask:
    claim = Claim(
        id=ClaimId(uuid4()),
        tenant_id=tenant_id,
        source=SourceObject(
            bucket=BUCKET,
            key=f"tenants/{tenant_id}/claims/unclear.pdf",
            etag=f"etag-{tenant_id}",
            size=2048,
        ),
        status=ClaimStatus.REVIEW_PENDING,
        redacted=await FakePiiRedactor().redact(read_note("unclear")),
        created_at=NOW,
        updated_at=NOW,
    )
    await harness.uow.claims.add(claim)
    task = ReviewTask(
        id=uuid4(),
        claim_id=claim.id,
        tenant_id=claim.tenant_id,
        reason=ReviewReason.LOW_CONFIDENCE,
        created_at=NOW,
    )
    await harness.uow.review_tasks.add(task)
    return task


async def test_the_queue_lists_open_reviews_with_the_redacted_note(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    task = await seed_review(harness)

    async with harness.client() as client:
        response = await client.get("/v1/reviews", headers=api_headers)

    assert response.status_code == 200
    (body,) = response.json()
    assert body["task_id"] == str(task.id)
    assert body["claim_id"] == str(task.claim_id)
    assert body["claim_status"] == "REVIEW_PENDING"
    assert "<PERSON>" in body["redacted_text"]
    assert_no_pii(response.text)


async def test_the_queue_never_lists_another_tenants_reviews(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    await seed_review(harness, tenant_id="tenant-b")

    async with harness.client() as client:
        response = await client.get("/v1/reviews", headers=api_headers)

    assert response.status_code == 200
    assert response.json() == []


async def test_the_limit_is_bounded(harness: ApiHarness, api_headers: dict[str, str]) -> None:
    async with harness.client() as client:
        too_small = await client.get("/v1/reviews?limit=0", headers=api_headers)
        too_large = await client.get("/v1/reviews?limit=201", headers=api_headers)

    assert too_small.status_code == 422
    assert too_large.status_code == 422


async def test_the_queue_needs_the_api_key_and_the_tenant_header(harness: ApiHarness) -> None:
    async with harness.client() as client:
        no_key = await client.get("/v1/reviews", headers={"X-Tenant-Id": "tenant-a"})
        no_tenant = await client.get("/v1/reviews", headers={"X-API-Key": API_KEY})

    assert no_key.status_code == 401
    assert no_tenant.status_code == 422


async def test_resolving_a_review_delivers_the_decision_as_human(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    task = await seed_review(harness)

    async with harness.client() as client:
        response = await client.post(
            f"/v1/reviews/{task.id}/resolve", json=RESOLUTION, headers=api_headers
        )

    assert response.status_code == 200
    assert response.json() == {
        "task_id": str(task.id),
        "claim_id": str(task.claim_id),
        "claim_status": "REVIEW_RESOLVED",
    }
    ((_, payload),) = harness.webhook.deliveries
    assert payload.decided_by == "human"
    assert payload.outcome == "MEETS_NECESSITY"
    assert harness.uow.review_tasks.tasks[task.id].status is ReviewStatus.RESOLVED
    assert_no_pii(payload.model_dump_json())


async def test_resolving_under_another_tenants_header_is_404(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    task = await seed_review(harness)

    async with harness.client() as client:
        response = await client.post(
            f"/v1/reviews/{task.id}/resolve",
            json=RESOLUTION,
            headers={**api_headers, "X-Tenant-Id": "tenant-b"},
        )

    # 404, not 403: a 403 would confirm the task exists.
    assert response.status_code == 404
    assert harness.webhook.deliveries == []


async def test_resolving_an_unknown_task_is_404(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            f"/v1/reviews/{uuid4()}/resolve", json=RESOLUTION, headers=api_headers
        )

    assert response.status_code == 404


async def test_resolving_twice_is_409(harness: ApiHarness, api_headers: dict[str, str]) -> None:
    task = await seed_review(harness)

    async with harness.client() as client:
        first = await client.post(
            f"/v1/reviews/{task.id}/resolve", json=RESOLUTION, headers=api_headers
        )
        second = await client.post(
            f"/v1/reviews/{task.id}/resolve", json=RESOLUTION, headers=api_headers
        )

    assert first.status_code == 200
    assert second.status_code == 409
    assert len(harness.webhook.deliveries) == 1


async def test_insufficient_evidence_is_not_a_human_resolution(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    task = await seed_review(harness)

    async with harness.client() as client:
        response = await client.post(
            f"/v1/reviews/{task.id}/resolve",
            json={**RESOLUTION, "resolution": "INSUFFICIENT_EVIDENCE"},
            headers=api_headers,
        )

    assert response.status_code == 422
    assert harness.webhook.deliveries == []


async def test_a_failed_delivery_still_answers_200_with_the_parked_status(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    # The reviewer's decision is recorded; the delivery is an operator's problem now.
    task = await seed_review(harness)
    harness.webhook.error = WebhookTransientError("3 attempts failed")

    async with harness.client() as client:
        response = await client.post(
            f"/v1/reviews/{task.id}/resolve", json=RESOLUTION, headers=api_headers
        )

    assert response.status_code == 200
    assert response.json()["claim_status"] == "NOTIFY_FAILED"
