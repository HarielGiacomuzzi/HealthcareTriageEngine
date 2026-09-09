"""The fake gateway is what `docker compose up` runs by default, so it is production
code for the demo, not a test double — hence a real test of its rule table."""

from uuid import uuid4

import pytest
from tests.fakes import build_evaluation_request

from ecet.domain.evaluation import Decision
from ecet.domain.ids import PolicyId
from ecet.infrastructure.llm.fake_gateway import FakeLlmGateway


@pytest.mark.parametrize(
    ("text", "decision", "confidence"),
    [
        ("The request was denied by the reviewer.", Decision.DOES_NOT_MEET, 0.92),
        ("Duration unclear and not documented.", Decision.INSUFFICIENT_EVIDENCE, 0.4),
        ("Nine weeks of conservative therapy documented.", Decision.MEETS_NECESSITY, 0.91),
    ],
)
async def test_the_rule_table(text: str, decision: Decision, confidence: float) -> None:
    evaluation = await FakeLlmGateway().evaluate(build_evaluation_request(redacted_text=text))

    assert evaluation.decision is decision
    assert evaluation.confidence == confidence


async def test_the_rules_are_case_insensitive() -> None:
    evaluation = await FakeLlmGateway().evaluate(build_evaluation_request(redacted_text="DENIED."))

    assert evaluation.decision is Decision.DOES_NOT_MEET


async def test_a_decisive_answer_matches_the_first_policy() -> None:
    policy_id = PolicyId(uuid4())
    request = build_evaluation_request(policy_id=policy_id, redacted_text="therapy documented")

    evaluation = await FakeLlmGateway().evaluate(request)

    assert evaluation.matched_policy_id == policy_id


async def test_an_insufficient_answer_matches_no_policy() -> None:
    request = build_evaluation_request(redacted_text="duration unclear")

    evaluation = await FakeLlmGateway().evaluate(request)

    assert evaluation.matched_policy_id is None


async def test_the_vendor_metadata_is_filled_in() -> None:
    request = build_evaluation_request(redacted_text="therapy documented")

    evaluation = await FakeLlmGateway().evaluate(request)

    assert evaluation.model == FakeLlmGateway.MODEL
    assert evaluation.prompt_version == request.prompt_version
    assert [code.code for code in evaluation.cited_codes] == request.found_codes
