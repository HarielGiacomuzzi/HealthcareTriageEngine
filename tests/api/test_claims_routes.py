from uuid import uuid4

from tests.api.conftest import API_KEY, BUCKET, KEY, ApiHarness
from tests.pii import assert_no_pii

from ecet.domain.claim import ClaimStatus


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
