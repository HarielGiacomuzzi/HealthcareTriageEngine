from tests.api.conftest import ApiHarness


async def test_readyz_is_200_when_every_probe_passes(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"database": True, "queue": True, "redactor": True}


async def test_readyz_is_503_when_a_probe_fails(harness: ApiHarness) -> None:
    harness.probe_results["queue"] = False

    async with harness.client() as client:
        response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["queue"] is False


async def test_a_raising_probe_reports_false_rather_than_500(harness: ApiHarness) -> None:
    async def explode() -> bool:
        raise RuntimeError("connection refused")

    harness.container.probes["database"] = explode

    async with harness.client() as client:
        response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["database"] is False


async def test_readyz_with_no_probes_is_not_ready(harness: ApiHarness) -> None:
    """`all([])` is True. A container that registered nothing has checked nothing, and
    "nothing checked" is not "ready"."""
    harness.container.probes.clear()

    async with harness.client() as client:
        response = await client.get("/readyz")

    assert response.status_code == 503
