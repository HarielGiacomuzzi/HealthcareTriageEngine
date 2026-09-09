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
    assert "mc mb -p local/claims" in script
    assert "auth_token=" in script
    assert setup["depends_on"]["api"]["condition"] == "service_healthy"
    assert setup["depends_on"]["minio"]["condition"] == "service_healthy"


def test_the_api_waits_for_minio_too(compose: dict[str, Any]) -> None:
    assert "minio" in compose["services"]["api"]["depends_on"]


def test_the_mock_client_is_built_and_exposed(compose: dict[str, Any]) -> None:
    mock = compose["services"]["mock-client"]

    assert mock["build"] == "services/mock-client"
    assert "8081:8081" in mock["ports"]
    assert "MOCK_CLIENT_SECRETS" in mock["environment"]


def test_the_worker_waits_for_the_api_so_migrations_have_run(compose: dict[str, Any]) -> None:
    # The worker never migrates; a healthy api is the signal that the schema is at head.
    worker = compose["services"]["worker"]

    assert worker["depends_on"]["api"]["condition"] == "service_healthy"
    assert worker["depends_on"]["rabbitmq"]["condition"] == "service_healthy"


def test_the_worker_can_reach_the_mock_client(compose: dict[str, Any]) -> None:
    # The seeded tenants' webhook URLs point at http://mock-client:8081/hooks/...
    assert "mock-client" in compose["services"]["worker"]["depends_on"]


def test_the_minio_webhook_target_is_persistent(compose: dict[str, Any]) -> None:
    # Phase 3 carry-over: without `queue_dir` MinIO's webhook target is fire-and-forget,
    # so an event the api answered 4xx is gone — no claim row, no retry, no signal.
    script = " ".join(compose["services"]["minio-setup"]["entrypoint"])

    assert "queue_dir=" in script
    assert "queue_limit=" in script


def test_the_rabbitmq_healthcheck_does_not_run_as_root(compose: dict[str, Any]) -> None:
    # `docker exec` runs a healthcheck as root, and `rabbitmq-diagnostics` creates
    # $HOME/.erlang.cookie mode 0400 owned by whoever runs it. On a fresh volume the
    # probe can win that race against the booting server, which then cannot read its
    # own cookie and exits with `eacces`. Dropping to `rabbitmq` makes the probe write
    # the cookie the server already expects.
    probe = compose["services"]["rabbitmq"]["healthcheck"]["test"]

    assert probe[:3] == ["CMD", "gosu", "rabbitmq"]
