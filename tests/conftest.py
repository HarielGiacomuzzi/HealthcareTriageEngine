import os

import pytest

from ecet.config import Settings


@pytest.fixture(autouse=True)
def _no_ambient_ecet_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Settings reads os.environ; no ambient ECET_* var may reach a test."""
    for name in [key for key in os.environ if key.startswith("ECET_")]:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def settings() -> Settings:
    """Valid settings built from class defaults only — no .env file, no ambient ECET_* env vars."""
    return Settings(_env_file=None, s3_event_token="test-token", api_key="test-key")
