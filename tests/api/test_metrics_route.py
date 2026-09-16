"""`/metrics` is unauthenticated on purpose: it is scraped by a sidecar inside the
compose network, it carries no claim content, and an API key in a Prometheus config
is a key in one more place."""

from tests.api.conftest import ApiHarness


async def test_metrics_is_served_without_an_api_key(harness: ApiHarness) -> None:
    # `Mount` only matches a sub-path (`path_regex` requires the trailing "/"), so a
    # bare GET /metrics 307s to /metrics/ first, exactly as it would for curl or a
    # Prometheus scraper — both follow redirects, so the client here does too.
    async with harness.client(follow_redirects=True) as client:
        response = await client.get("/metrics")

    assert response.status_code == 200
    assert "ecet_ingest_seconds" in response.text
    assert response.headers["content-type"].startswith("text/plain")


async def test_metrics_exposes_no_claim_text(harness: ApiHarness) -> None:
    """Belt and braces for ADR-001: the exposition is a public-ish surface, and the
    only way text could reach it is a label somebody added by mistake."""
    async with harness.client(follow_redirects=True) as client:
        response = await client.get("/metrics")

    assert "claim_id" not in response.text
