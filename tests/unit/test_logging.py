import json

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
