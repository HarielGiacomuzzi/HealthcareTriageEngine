from uuid import uuid4

from tests.api.conftest import API_KEY, BUCKET, KEY, NOW, ApiHarness
from tests.pii import assert_no_pii

from ecet.application.errors import WebhookTransientError
from ecet.domain.claim import Claim, ClaimStatus, SourceObject
from ecet.domain.evaluation import Decision, Evaluation
from ecet.domain.ids import ClaimId


async def test_manual_ingest_fills_the_etag_and_size_from_head(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/claims/ingest", json={"bucket": BUCKET, "key": KEY}, headers=api_headers
        )

    assert response.status_code == 200
    assert response.json()["status"] == ClaimStatus.QUEUED.value
    (stored,) = harness.uow.claims.claims.values()
    assert stored.source.size == len(b"%PDF-1.7 fake bytes")
    assert stored.source.etag


async def test_manual_ingest_of_a_missing_object_is_404(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/claims/ingest",
            json={"bucket": BUCKET, "key": "tenants/tenant-a/claims/absent.pdf"},
            headers=api_headers,
        )

    assert response.status_code == 404
    assert "absent.pdf" not in response.text


async def test_a_claim_can_be_read_back(harness: ApiHarness, api_headers: dict[str, str]) -> None:
    async with harness.client() as client:
        created = await client.post(
            "/v1/claims/ingest", json={"bucket": BUCKET, "key": KEY}, headers=api_headers
        )
        claim_id = created.json()["claim_id"]

        response = await client.get(f"/v1/claims/{claim_id}", headers=api_headers)

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == claim_id
    assert body["tenant_id"] == "tenant-a"
    assert body["status"] == ClaimStatus.QUEUED.value
    assert body["deterministic"]["verdict"] == "PASS"
    assert body["entity_counts"]["US_SSN"] == 1


async def test_the_claim_view_exposes_exactly_its_declared_fields(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        created = await client.post(
            "/v1/claims/ingest", json={"bucket": BUCKET, "key": KEY}, headers=api_headers
        )
        response = await client.get(f"/v1/claims/{created.json()['claim_id']}", headers=api_headers)

    body = response.json()
    assert set(body) == {
        "id",
        "tenant_id",
        "status",
        "policy_ids",
        "entity_counts",
        "deterministic",
        "evaluation",
        "failure_reason",
        "created_at",
        "updated_at",
    }


async def test_another_tenants_claim_is_404_not_403(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        created = await client.post(
            "/v1/claims/ingest", json={"bucket": BUCKET, "key": KEY}, headers=api_headers
        )
        response = await client.get(
            f"/v1/claims/{created.json()['claim_id']}",
            headers={**api_headers, "X-Tenant-Id": "tenant-b"},
        )

    # 404, not 403: a 403 confirms the claim exists, which is itself a leak.
    assert response.status_code == 404


async def test_an_unknown_claim_is_404(harness: ApiHarness, api_headers: dict[str, str]) -> None:
    async with harness.client() as client:
        response = await client.get(f"/v1/claims/{uuid4()}", headers=api_headers)

    assert response.status_code == 404


async def test_reading_a_claim_needs_the_tenant_header(
    harness: ApiHarness,
) -> None:
    async with harness.client() as client:
        response = await client.get(f"/v1/claims/{uuid4()}", headers={"X-API-Key": API_KEY})

    assert response.status_code == 422


async def test_manual_ingest_of_a_zero_byte_object_is_400_not_500(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    harness.storage.put(BUCKET, "tenants/tenant-a/claims/empty.pdf", b"")

    async with harness.client() as client:
        response = await client.post(
            "/v1/claims/ingest",
            json={"bucket": BUCKET, "key": "tenants/tenant-a/claims/empty.pdf"},
            headers=api_headers,
        )

    assert response.status_code == 400


async def test_manual_ingest_refuses_a_bucket_the_deployment_does_not_own(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    # Without this the endpoint HEADs any bucket the caller names — a size/existence
    # oracle over the whole MinIO instance, since v1 has one global API key.
    async with harness.client() as client:
        response = await client.post(
            "/v1/claims/ingest",
            json={"bucket": "someone-elses-bucket", "key": KEY},
            headers=api_headers,
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "No object at that bucket and key."
    assert harness.storage.reads == []
    assert_no_pii(response.text)


async def seed_notify_failed(harness: ApiHarness) -> ClaimId:
    claim = Claim(
        id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source=SourceObject(
            bucket=BUCKET, key="tenants/tenant-a/claims/stuck.pdf", etag="etag-stuck", size=2048
        ),
        status=ClaimStatus.NOTIFY_FAILED,
        evaluation=Evaluation(
            decision=Decision.MEETS_NECESSITY,
            confidence=0.91,
            model="fake-deterministic",
            prompt_version="v1",
        ),
        failure_reason="webhook_unreachable",
        notification_attempts=1,
        created_at=NOW,
        updated_at=NOW,
    )
    await harness.uow.claims.add(claim)
    return claim.id


async def test_retry_notify_delivers_and_approves(harness: ApiHarness) -> None:
    claim_id = await seed_notify_failed(harness)

    async with harness.client() as client:
        response = await client.post(
            f"/v1/claims/{claim_id}/retry-notify", headers={"X-API-Key": API_KEY}
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "APPROVED_AUTO"
    assert body["failure_reason"] is None
    ((_, payload),) = harness.webhook.deliveries
    assert payload.decided_by == "auto"
    assert_no_pii(payload.model_dump_json())


async def test_a_retry_that_fails_again_answers_502_with_the_claim(harness: ApiHarness) -> None:
    claim_id = await seed_notify_failed(harness)
    harness.webhook.error = WebhookTransientError("3 attempts failed")

    async with harness.client() as client:
        response = await client.post(
            f"/v1/claims/{claim_id}/retry-notify", headers={"X-API-Key": API_KEY}
        )

    # 502 so a `curl -f` in a script notices; the body is still the claim.
    assert response.status_code == 502
    body = response.json()
    assert body["status"] == "NOTIFY_FAILED"
    assert body["failure_reason"] == "webhook_unreachable"
    assert harness.uow.claims.claims[claim_id].notification_attempts == 2
    assert_no_pii(response.text)


async def test_retry_notify_on_a_claim_that_did_not_fail_is_409(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        created = await client.post(
            "/v1/claims/ingest", json={"bucket": BUCKET, "key": KEY}, headers=api_headers
        )
        response = await client.post(
            f"/v1/claims/{created.json()['claim_id']}/retry-notify",
            headers={"X-API-Key": API_KEY},
        )

    assert response.status_code == 409
    assert harness.webhook.deliveries == []


async def test_retry_notify_on_an_unknown_claim_is_404(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.post(
            f"/v1/claims/{uuid4()}/retry-notify", headers={"X-API-Key": API_KEY}
        )

    assert response.status_code == 404


async def test_retry_notify_needs_the_api_key(harness: ApiHarness) -> None:
    claim_id = await seed_notify_failed(harness)

    async with harness.client() as client:
        response = await client.post(f"/v1/claims/{claim_id}/retry-notify")

    assert response.status_code == 401
    assert harness.webhook.deliveries == []
