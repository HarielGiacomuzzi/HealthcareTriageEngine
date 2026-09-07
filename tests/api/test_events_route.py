import json
from pathlib import Path
from typing import Any

from tests.api.conftest import ApiHarness

from ecet.domain.claim import ClaimStatus

EVENTS_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "s3_events"


def load_event(name: str) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads((EVENTS_DIR / f"{name}.json").read_text())
    return payload


def put_event(key: str = "tenants/tenant-a/claims/note-1.pdf", **overrides: Any) -> dict[str, Any]:
    event = load_event("minio_put")
    event["Records"][0]["s3"]["object"]["key"] = key
    event["Records"][0]["s3"]["object"].update(overrides)
    return event


async def test_a_put_event_ingests_the_claim(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=put_event(), headers=event_headers)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == ClaimStatus.QUEUED.value
    assert body["duplicate"] is False
    assert len(harness.queue.published) == 1


async def test_the_url_encoded_key_is_unescaped_before_use(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    key = "tenants/tenant-a/claims/note+with+spaces.pdf"
    harness.storage.put("claims", "tenants/tenant-a/claims/note with spaces.pdf", b"%PDF-1.7 x")

    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3", json=put_event(key=key), headers=event_headers
        )

    assert response.status_code == 200
    assert ("claims", "tenants/tenant-a/claims/note with spaces.pdf") in harness.storage.reads


async def test_a_delete_event_is_ignored(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3", json=load_event("minio_delete"), headers=event_headers
        )

    assert response.status_code == 200
    assert response.json() == {"ignored": 1}
    assert harness.uow.claims.claims == {}


async def test_a_non_pdf_key_is_ignored(harness: ApiHarness, event_headers: dict[str, str]) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3",
            json=put_event(key="tenants/tenant-a/claims/notes.txt"),
            headers=event_headers,
        )

    assert response.status_code == 200
    assert response.json() == {"ignored": 1}


async def test_an_empty_records_list_is_ignored(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json={"Records": []}, headers=event_headers)

    assert response.status_code == 200


async def test_a_key_outside_the_tenant_layout_is_400(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3", json=put_event(key="uploads/note-1.pdf"), headers=event_headers
        )

    assert response.status_code == 400
    assert "uploads/note-1.pdf" not in response.text


async def test_a_tenant_with_no_policies_is_422(event_headers: dict[str, str]) -> None:
    harness = ApiHarness()
    harness.uow.policies.policies.clear()

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=put_event(), headers=event_headers)

    assert response.status_code == 422
    assert response.json()["title"] == "No active policies"


async def test_an_unknown_tenant_is_404(event_headers: dict[str, str]) -> None:
    harness = ApiHarness(tenants=[])

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=put_event(), headers=event_headers)

    assert response.status_code == 404


async def test_multiple_records_answer_207_with_one_result_each(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    harness.storage.put("claims", "tenants/tenant-a/claims/note-2.pdf", b"%PDF-1.7 y")
    first = put_event()
    second = put_event(key="tenants/tenant-a/claims/note-2.pdf", eTag="etag-2")
    envelope = {"Records": [first["Records"][0], second["Records"][0]]}

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=envelope, headers=event_headers)

    assert response.status_code == 207
    results = response.json()["results"]
    assert len(results) == 2
    assert all(entry["result"]["status"] == "QUEUED" for entry in results)


async def test_one_failing_record_does_not_abort_the_batch(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    good = put_event()
    bad = put_event(key="tenants/tenant-a/claims/absent.pdf", eTag="etag-3")
    envelope = {"Records": [bad["Records"][0], good["Records"][0]]}

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=envelope, headers=event_headers)

    assert response.status_code == 207
    results = response.json()["results"]
    assert results[0]["error"] == "ExtractionFailed"
    assert results[1]["result"]["status"] == "QUEUED"
