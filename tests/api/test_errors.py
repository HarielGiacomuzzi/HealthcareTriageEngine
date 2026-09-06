import httpx
import pytest
from fastapi import FastAPI

from ecet.application.errors import ExtractionFailed, QueuePublishError
from ecet.domain.errors import (
    ClaimNotFound,
    DomainError,
    InvalidObjectKey,
    NoPoliciesForTenant,
    PdfTooLarge,
    TenantNotFound,
)
from ecet.interfaces.api.errors import register_error_handlers


def build_app(error: Exception) -> FastAPI:
    app = FastAPI()
    register_error_handlers(app)

    @app.get("/boom")
    async def boom() -> None:
        raise error

    return app


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (InvalidObjectKey("expected tenants/... got uploads/secret-patient-name.pdf"), 400),
        (PdfTooLarge("40000000 bytes exceeds the 20000000 byte limit"), 413),
        (TenantNotFound("tenant-x"), 404),
        (ExtractionFailed("no_text"), 422),
        (NoPoliciesForTenant("tenant-empty"), 422),
        (ClaimNotFound("00000000-0000-4000-8000-000000000000"), 404),
        (QueuePublishError("no confirm"), 503),
    ],
)
async def test_each_domain_error_maps_to_its_status(error: DomainError, status: int) -> None:
    transport = httpx.ASGITransport(app=build_app(error), raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/boom")

    assert response.status_code == status
    body = response.json()
    assert body["status"] == status
    assert body["title"]
    assert response.headers["content-type"].startswith("application/problem+json")


async def test_the_detail_never_echoes_the_exception_message() -> None:
    error = InvalidObjectKey("expected tenants/... got uploads/secret-patient-name.pdf")
    transport = httpx.ASGITransport(app=build_app(error), raise_app_exceptions=False)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/boom")

    assert "secret-patient-name" not in response.text


async def test_an_unmapped_domain_error_is_a_500() -> None:
    class Surprise(DomainError):
        pass

    transport = httpx.ASGITransport(app=build_app(Surprise("x")), raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/boom")

    assert response.status_code == 500
    assert response.json()["title"] == "Internal error"
