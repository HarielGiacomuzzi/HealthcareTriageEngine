import json
import logging

import pytest
import structlog

from ecet.infrastructure.observability.logging import (
    configure_logging,
    drop_sensitive_fields,
)


def test_sensitive_fields_are_dropped() -> None:
    event_dict = {
        "event": "claim.ingested",
        "claim_id": "c-1",
        "text": "Patient John Doe, SSN 123-45-6789",
        "raw_text": "same",
        "redacted_text": "same",
        "notes": "reviewer notes with PII",
        "webhook_secret": "shh",
        "api_key": "shh",
    }

    result = drop_sensitive_fields(None, "info", event_dict)

    assert result == {"event": "claim.ingested", "claim_id": "c-1"}


def test_non_sensitive_fields_survive() -> None:
    event_dict = {"event": "worker.started", "env": "dev"}

    assert drop_sensitive_fields(None, "info", dict(event_dict)) == {
        "event": "worker.started",
        "env": "dev",
    }


def test_configure_logging_emits_json_without_secrets(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")

    structlog.get_logger("test").info("claim.transition", claim_id="c-1", text="Patient John Doe")

    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["event"] == "claim.transition"
    assert payload["claim_id"] == "c-1"
    assert payload["level"] == "info"
    assert "text" not in payload
    assert "John Doe" not in line


def test_stdlib_records_are_rendered_as_json_through_the_guard(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Phase 0 carry-over #12: `cli.py api` hands uvicorn `log_config=None`, so
    `uvicorn.access` logs through the stdlib root handler. That handler has to be
    structlog's, or ADR-001's guard is bypassed by every third-party library."""
    configure_logging("INFO")

    logging.getLogger("uvicorn.access").info(
        "request finished", extra={"text": "Patient John Doe", "path": "/v1/claims/ingest"}
    )

    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["event"] == "request finished"
    assert payload["logger"] == "uvicorn.access"
    assert payload["path"] == "/v1/claims/ingest"
    assert "text" not in payload
    assert "John Doe" not in line


def test_the_bound_request_id_reaches_a_stdlib_record(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")
    structlog.contextvars.bind_contextvars(request_id="req-1")
    try:
        logging.getLogger("uvicorn.error").warning("startup complete")
    finally:
        structlog.contextvars.clear_contextvars()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["request_id"] == "req-1"
