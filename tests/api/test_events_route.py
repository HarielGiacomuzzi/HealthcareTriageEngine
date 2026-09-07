import json
from pathlib import Path
from typing import Any

from tests.api.conftest import ApiHarness

from ecet.domain.claim import ClaimStatus
from ecet.interfaces.api.routes.events import MAX_RECORDS

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
    # Indexed, not the shared (and attacker-controlled) bucket name.
    assert [entry["index"] for entry in results] == [0, 1]
    assert all("bucket" not in entry for entry in results)


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


async def test_207_index_is_the_position_in_records_not_in_the_filtered_list(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    harness.storage.put("claims", "tenants/tenant-a/claims/note-2.pdf", b"%PDF-1.7 y")
    ignored_record = put_event(key="tenants/tenant-a/claims/notes.txt")["Records"][0]
    first = put_event()["Records"][0]
    second = put_event(key="tenants/tenant-a/claims/note-2.pdf", eTag="etag-2")["Records"][0]
    envelope = {"Records": [ignored_record, first, second]}

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=envelope, headers=event_headers)

    assert response.status_code == 207
    results = response.json()["results"]
    assert [entry["index"] for entry in results] == [1, 2]


async def test_an_unexpected_exception_on_one_record_does_not_abort_the_batch(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    # eTag "" fails `SourceObject`'s `min_length=1` with a pydantic ValidationError,
    # not a DomainError — exactly the shape a bug elsewhere in the pipeline would take.
    bad = put_event(eTag="")
    good = put_event(key="tenants/tenant-a/claims/note-2.pdf", eTag="etag-2")
    harness.storage.put("claims", "tenants/tenant-a/claims/note-2.pdf", b"%PDF-1.7 y")
    envelope = {"Records": [bad["Records"][0], good["Records"][0]]}

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=envelope, headers=event_headers)

    assert response.status_code == 207
    results = response.json()["results"]
    assert results[0]["error"] == "ValidationError"
    assert results[1]["result"]["status"] == "QUEUED"


async def test_a_record_with_zero_size_is_400_not_500(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=put_event(size=0), headers=event_headers)

    assert response.status_code == 400


async def test_a_record_with_a_missing_etag_is_400_not_500(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    event = put_event()
    del event["Records"][0]["s3"]["object"]["eTag"]

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=event, headers=event_headers)

    assert response.status_code == 400


async def test_a_record_with_an_empty_etag_is_400_not_500(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3", json=put_event(eTag=""), headers=event_headers
        )

    assert response.status_code == 400


async def test_more_than_the_record_cap_is_400(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    record = put_event()["Records"][0]
    envelope = {"Records": [record] * (MAX_RECORDS + 1)}

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=envelope, headers=event_headers)

    assert response.status_code == 400


async def test_an_uppercase_pdf_extension_is_ignored_not_400(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    # The filter and `OBJECT_KEY_RE` must agree on case-sensitivity: before this fix
    # the filter's `.lower()` let `NOTE-1.PDF` through as "actionable", and the
    # domain's case-sensitive regex then 400'd it. Filtering it out up front (ignored,
    # 200) is consistent with "not a claim pdf" rather than "a claim pdf we reject".
    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3",
            json=put_event(key="tenants/tenant-a/claims/NOTE-1.PDF"),
            headers=event_headers,
        )

    assert response.status_code == 200
    assert response.json() == {"ignored": 1}


async def test_malformed_request_body_still_gets_a_plain_422(
    event_headers: dict[str, str], harness: ApiHarness
) -> None:
    # FastAPI's own request-body validation (`RequestValidationError`) is a distinct
    # class from `pydantic_core.ValidationError` raised by hand inside a route — the
    # new mapping for the latter must not interfere with the former.
    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3", json={"Records": "not-a-list"}, headers=event_headers
        )

    assert response.status_code == 422
