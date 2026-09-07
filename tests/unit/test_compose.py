"""The compose file is configuration, so it gets a configuration test: the pieces the
phase depends on are present and point at the right things. Cheap insurance against a
silent edit — the alternative is finding out during a demo."""

from pathlib import Path
from typing import Any

import pytest
import yaml

COMPOSE = Path(__file__).resolve().parents[2] / "docker-compose.yml"


@pytest.fixture(scope="module")
def compose() -> dict[str, Any]:
    parsed: dict[str, Any] = yaml.safe_load(COMPOSE.read_text())
    return parsed


def test_the_api_healthcheck_probes_readyz(compose: dict[str, Any]) -> None:
    probe = " ".join(compose["services"]["api"]["healthcheck"]["test"])

    assert "/readyz" in probe
    assert "/healthz" not in probe


def test_minio_setup_registers_the_webhook_and_the_event(compose: dict[str, Any]) -> None:
    setup = compose["services"]["minio-setup"]
    script = " ".join(setup["entrypoint"])

    assert "notify_webhook:ecet" in script
    assert "/v1/events/s3" in script
    assert "mc event add" in script
    assert "--suffix .pdf" in script
    assert setup["depends_on"]["api"]["condition"] == "service_healthy"
    assert setup["depends_on"]["minio"]["condition"] == "service_healthy"


def test_the_api_waits_for_minio_too(compose: dict[str, Any]) -> None:
    assert "minio" in compose["services"]["api"]["depends_on"]
