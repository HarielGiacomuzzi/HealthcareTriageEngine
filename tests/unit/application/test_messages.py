"""The message is the only thing that crosses the process boundary, so its shape is
tested on its own — a field renamed here silently breaks the Phase 4 worker."""

import json
from datetime import UTC, date, datetime
from uuid import uuid4

from tests.pii import assert_no_pii

from ecet.application.messages import EvaluationMessage, PolicySnapshot
from ecet.domain.ids import ClaimId, PolicyId, TenantId
from ecet.domain.policy import Policy

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


def build_policy() -> Policy:
    return Policy.model_validate(
        {
            "id": PolicyId(uuid4()),
            "tenant_id": "tenant-a",
            "name": "MRI lumbar spine",
            "version": 2,
            "covered_codes": [{"code": "M54.5"}, {"code": "M51.26"}],
            "excluded_codes": [{"code": "Z00.00"}],
            "criteria_text": "Imaging is covered after six weeks of conservative therapy.",
            "required_evidence": ["clinical examination note"],
            "effective_from": date(2026, 1, 1),
        }
    )


def test_policy_snapshot_flattens_the_code_sets_deterministically() -> None:
    snapshot = PolicySnapshot.of(build_policy())

    assert snapshot.covered_codes == ["M51.26", "M54.5"]
    assert snapshot.excluded_codes == ["Z00.00"]
    assert snapshot.required_evidence == ["clinical examination note"]


def test_the_serialised_message_carries_no_raw_text_and_no_secret() -> None:
    message = EvaluationMessage(
        message_id=uuid4(),
        claim_id=ClaimId(uuid4()),
        tenant_id=TenantId("tenant-a"),
        redacted_text="Patient <PERSON> with M54.5.",
        entity_counts={"PERSON": 1},
        policies=[PolicySnapshot.of(build_policy())],
        deterministic_verdict="PASS",
        found_codes=["M54.5"],
        enqueued_at=NOW,
    )

    body = json.loads(message.model_dump_json())

    assert body["schema_version"] == 1
    assert set(body) == {
        "schema_version",
        "message_id",
        "claim_id",
        "tenant_id",
        "redacted_text",
        "entity_counts",
        "policies",
        "deterministic_verdict",
        "found_codes",
        "enqueued_at",
    }
    assert "raw_text" not in body
    assert "webhook_secret" not in body
    assert "webhook_url" not in body
    assert_no_pii(message.model_dump_json())


def test_it_round_trips_through_json() -> None:
    message = EvaluationMessage(
        message_id=uuid4(),
        claim_id=ClaimId(uuid4()),
        tenant_id=TenantId("tenant-a"),
        redacted_text="Patient <PERSON> with M54.5.",
        policies=[PolicySnapshot.of(build_policy())],
        deterministic_verdict="UNCERTAIN",
        found_codes=[],
        enqueued_at=NOW,
    )

    assert EvaluationMessage.model_validate_json(message.model_dump_json()) == message
