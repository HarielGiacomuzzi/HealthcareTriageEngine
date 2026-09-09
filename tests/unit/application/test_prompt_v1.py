"""The prompt is application-owned, so it is testable without a vendor. What matters
here is that every fact the model needs is in the message and nothing it must not see
is."""

from uuid import uuid4

from tests.pii import assert_no_pii

from ecet.application.messages import PolicySnapshot
from ecet.application.ports.llm_gateway import EvaluationOutput, EvaluationRequest
from ecet.application.prompts.evaluate_v1 import (
    PROMPT_VERSION,
    SYSTEM_PROMPT,
    TOOL,
    TOOL_NAME,
    build_messages,
    render_user_message,
)
from ecet.domain.ids import ClaimId, PolicyId

POLICY_ID = PolicyId(uuid4())


def build_request() -> EvaluationRequest:
    return EvaluationRequest(
        claim_id=ClaimId(uuid4()),
        redacted_text="Patient <PERSON> with M54.5 after nine weeks of therapy.",
        policies=[
            PolicySnapshot(
                id=POLICY_ID,
                name="MRI lumbar spine",
                version=2,
                covered_codes=["M51.26", "M54.5"],
                excluded_codes=["Z00.00"],
                criteria_text="Covered after six weeks of conservative therapy.",
                required_evidence=["conservative therapy >= 6 weeks"],
            )
        ],
        found_codes=["M54.5"],
        prompt_version=PROMPT_VERSION,
    )


def test_the_user_message_carries_every_policy_field_the_model_needs() -> None:
    rendered = render_user_message(build_request())

    assert str(POLICY_ID) in rendered
    assert "MRI lumbar spine" in rendered
    assert "M51.26" in rendered
    assert "Z00.00" in rendered
    assert "six weeks of conservative therapy" in rendered
    assert "conservative therapy >= 6 weeks" in rendered
    assert "M54.5" in rendered
    assert "Patient <PERSON>" in rendered


def test_the_rendered_prompt_carries_no_pii() -> None:
    assert_no_pii(render_user_message(build_request()))
    assert_no_pii(SYSTEM_PROMPT)


def test_build_messages_is_a_system_then_user_pair() -> None:
    messages = build_messages(build_request())

    assert [message["role"] for message in messages] == ["system", "user"]
    assert messages[0]["content"] == SYSTEM_PROMPT


def test_the_tool_schema_matches_the_output_model() -> None:
    assert TOOL["type"] == "function"
    assert TOOL["function"]["name"] == TOOL_NAME
    parameters = TOOL["function"]["parameters"]
    assert parameters == EvaluationOutput.model_json_schema()
    assert set(parameters["properties"]) == set(EvaluationOutput.model_fields)


def test_an_empty_policy_list_still_renders() -> None:
    request = build_request().model_copy(update={"policies": [], "found_codes": []})

    rendered = render_user_message(request)

    assert "none" in rendered.lower()
