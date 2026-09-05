import httpx

from ecet.config import Settings
from ecet.interfaces.api.app import create_app


async def test_healthz_returns_ok(settings: Settings) -> None:
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_create_app_keeps_settings_on_state(settings: Settings) -> None:
    app = create_app(settings)

    assert app.state.settings is settings
