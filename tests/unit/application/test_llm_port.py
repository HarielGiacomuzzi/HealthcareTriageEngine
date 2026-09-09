"""The gateway contract, tested at the boundary the adapters have to honour:
`EvaluationOutput` is what a vendor is allowed to return, `Evaluation` is what the
domain stores, and `to_evaluation` is the only bridge between them."""

from uuid import uuid4

import pytest
from tests.fakes import FakeLLMGateway

from ecet.application.ports.llm_gateway import (
    EvaluationOutput,
    EvaluationRequest,
    LLMGateway,
)
from ecet.domain.evaluation import Decision
from ecet.domain.ids import ClaimId, PolicyId


def build_output(**overrides: object) -> EvaluationOutput:
    fields: dict[str, object] = {
        "decision": Decision.MEETS_NECESSITY,
        "confidence": 0.91,
        "rationale": "Conservative therapy documented for nine weeks.",
        "cited_codes": ["M54.5"],
    }
    fields.update(overrides)
    return EvaluationOutput.model_validate(fields)


def test_the_fake_gateway_satisfies_the_port() -> None:
    assert isinstance(FakeLLMGateway(), LLMGateway)


def test_to_evaluation_carries_the_vendor_metadata() -> None:
    evaluation = build_output().to_evaluation(
        model="claude-sonnet-5",
        prompt_version="v1",
        latency_ms=412,
        input_tokens=900,
        output_tokens=120,
    )

    assert evaluation.decision is Decision.MEETS_NECESSITY
    assert evaluation.confidence == 0.91
    assert [code.code for code in evaluation.cited_codes] == ["M54.5"]
    assert evaluation.model == "claude-sonnet-5"
    assert evaluation.prompt_version == "v1"
    assert evaluation.latency_ms == 412
    assert evaluation.input_tokens == 900
    assert evaluation.output_tokens == 120


def test_a_missing_confidence_becomes_insufficient_evidence_at_zero() -> None:
    # evaluation.md: "vendor must supply; missing -> INSUFFICIENT_EVIDENCE, 0.0".
    evaluation = build_output(confidence=None).to_evaluation(
        model="m", prompt_version="v1", latency_ms=0, input_tokens=0, output_tokens=0
    )

    assert evaluation.decision is Decision.INSUFFICIENT_EVIDENCE
    assert evaluation.confidence == 0.0


def test_a_malformed_cited_code_is_dropped_not_fatal() -> None:
    evaluation = build_output(cited_codes=["M54.5", "not-a-code", ""]).to_evaluation(
        model="m", prompt_version="v1", latency_ms=0, input_tokens=0, output_tokens=0
    )

    assert [code.code for code in evaluation.cited_codes] == ["M54.5"]


def test_a_matched_policy_id_outside_the_request_is_discarded() -> None:
    known = PolicyId(uuid4())
    hallucinated = uuid4()  # a policy id that was never in the request

    evaluation = build_output(matched_policy_id=hallucinated).to_evaluation(
        model="m",
        prompt_version="v1",
        latency_ms=0,
        input_tokens=0,
        output_tokens=0,
        known_policy_ids={known},
    )

    assert evaluation.matched_policy_id is None


def test_an_evaluation_request_is_frozen() -> None:
    request = EvaluationRequest(
        claim_id=ClaimId(uuid4()),
        redacted_text="Patient <PERSON> with M54.5.",
        policies=[],
        found_codes=["M54.5"],
        prompt_version="v1",
    )

    with pytest.raises(Exception):  # noqa: B017 -- just needs to be immutable, not any one error type
        request.redacted_text = "mutated"  # type: ignore[misc]
