"""The vendor adapter, driven through `httpx.MockTransport` — no network, no key, and
in the default (non-slow) test run.

`respx` would do the same job; the spec names it, but a MockTransport handler is a
plain function and saves a dependency (see deviation 4).
"""

import json
from typing import Any

import httpx
import pytest
from tests.fakes import build_evaluation_request
from tests.pii import assert_no_pii

from ecet.application.errors import LLMInvalidOutput, LLMPermanentError, LLMTransientError
from ecet.application.ports.llm_gateway import LLMGateway
from ecet.domain.evaluation import Decision
from ecet.infrastructure.llm.openai_gateway import OpenAiLlmGateway

ARGUMENTS = json.dumps(
    {
        "decision": "MEETS_NECESSITY",
        "confidence": 0.91,
        "cited_codes": ["M54.5"],
        "rationale": "Nine weeks of conservative therapy documented.",
        "evidence_found": ["conservative therapy >= 6 weeks"],
        "evidence_missing": [],
    }
)


def completion_body(
    *, arguments: str = ARGUMENTS, tool_calls: bool = True, usage: bool = True
) -> dict[str, Any]:
    message: dict[str, Any] = {"role": "assistant", "content": None}
    if tool_calls:
        message["tool_calls"] = [
            {
                "id": "call_1",
                "type": "function",
                "function": {"name": "submit_evaluation", "arguments": arguments},
            }
        ]
    body: dict[str, Any] = {
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "created": 1,
        "model": "claude-sonnet-5",
        "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls"}],
    }
    if usage:
        body["usage"] = {"prompt_tokens": 900, "completion_tokens": 120, "total_tokens": 1020}
    return body


def build_gateway(handler: Any, *, model: str = "claude-sonnet-5") -> OpenAiLlmGateway:
    return OpenAiLlmGateway(
        base_url="http://vendor.invalid/v1/",
        api_key="test-key",
        model=model,
        timeout_s=5,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


def responder(status: int, body: dict[str, Any] | None = None) -> Any:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=body if body is not None else {"error": "x"})

    return handle


async def test_the_adapter_satisfies_the_port() -> None:
    assert isinstance(build_gateway(responder(200, completion_body())), LLMGateway)


async def test_a_tool_call_becomes_an_evaluation() -> None:
    gateway = build_gateway(responder(200, completion_body()))

    evaluation = await gateway.evaluate(build_evaluation_request())

    assert evaluation.decision is Decision.MEETS_NECESSITY
    assert evaluation.confidence == 0.91
    assert [code.code for code in evaluation.cited_codes] == ["M54.5"]
    assert evaluation.model == "claude-sonnet-5"
    assert evaluation.prompt_version == "v1"
    assert evaluation.input_tokens == 900
    assert evaluation.output_tokens == 120
    assert evaluation.latency_ms >= 0
    await gateway.aclose()


async def test_the_request_forces_the_tool_and_carries_no_pii() -> None:
    seen: list[dict[str, Any]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(200, json=completion_body())

    await build_gateway(handle).evaluate(build_evaluation_request())

    sent = seen[0]
    assert sent["model"] == "claude-sonnet-5"
    assert sent["temperature"] == 0
    assert sent["max_tokens"] == 1024
    assert sent["tool_choice"]["function"]["name"] == "submit_evaluation"
    assert sent["tools"][0]["function"]["name"] == "submit_evaluation"
    assert [message["role"] for message in sent["messages"]] == ["system", "user"]
    assert_no_pii(json.dumps(sent))


@pytest.mark.parametrize("status", [429, 500, 502, 503])
async def test_retryable_statuses_are_transient(status: int) -> None:
    with pytest.raises(LLMTransientError):
        await build_gateway(responder(status)).evaluate(build_evaluation_request())


@pytest.mark.parametrize("status", [400, 401, 403, 404])
async def test_client_errors_are_permanent(status: int) -> None:
    with pytest.raises(LLMPermanentError):
        await build_gateway(responder(status)).evaluate(build_evaluation_request())


async def test_a_connection_failure_is_transient() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(LLMTransientError):
        await build_gateway(handle).evaluate(build_evaluation_request())


async def test_a_response_with_no_tool_call_is_invalid_output() -> None:
    body = completion_body(tool_calls=False)

    with pytest.raises(LLMInvalidOutput):
        await build_gateway(responder(200, body)).evaluate(build_evaluation_request())


async def test_unparseable_arguments_are_invalid_output() -> None:
    body = completion_body(arguments="{not json")

    with pytest.raises(LLMInvalidOutput):
        await build_gateway(responder(200, body)).evaluate(build_evaluation_request())


async def test_arguments_that_break_the_schema_are_invalid_output() -> None:
    body = completion_body(arguments=json.dumps({"decision": "MAYBE", "confidence": 0.9}))

    with pytest.raises(LLMInvalidOutput):
        await build_gateway(responder(200, body)).evaluate(build_evaluation_request())


async def test_a_missing_usage_block_defaults_the_token_counts() -> None:
    # Local servers (Ollama, LM Studio) often omit `usage` entirely.
    body = completion_body(usage=False)

    evaluation = await build_gateway(responder(200, body)).evaluate(build_evaluation_request())

    assert evaluation.input_tokens == 0
    assert evaluation.output_tokens == 0


async def test_a_hallucinated_policy_id_is_discarded() -> None:
    arguments = json.dumps(
        {
            "decision": "MEETS_NECESSITY",
            "confidence": 0.9,
            "matched_policy_id": "11111111-1111-4111-8111-111111111111",
        }
    )

    evaluation = await build_gateway(responder(200, completion_body(arguments=arguments))).evaluate(
        build_evaluation_request()
    )

    assert evaluation.matched_policy_id is None
