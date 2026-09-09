"""One adapter for every OpenAI-compatible server (ADR-004).

There is no vendor branch in here on purpose: Anthropic's compatibility endpoint,
OpenAI, Azure, vLLM, Ollama, OpenRouter and LM Studio all speak
`/v1/chat/completions` with function calling, so the only difference between them is
`ECET_LLM_BASE_URL`, `ECET_LLM_API_KEY` and `ECET_LLM_MODEL`.

`max_retries=0` is deliberate. The SDK will happily retry a 429 for you, inside the
message handler, while the message's own delivery budget ticks — two retry policies
stacked. RabbitMQ owns retries in this system, so the SDK does none.

Nothing here redacts: the input was redacted at the trust boundary (UC-02, ADR-001)
and re-checking it at the wire would be theatre. Nothing here logs the prompt either.
"""

import time
from typing import Any, cast

import httpx
import structlog
from openai import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    AsyncOpenAI,
    InternalServerError,
    RateLimitError,
)
from openai.types.chat import ChatCompletionMessageParam
from pydantic import ValidationError

from ecet.application.errors import LLMInvalidOutput, LLMPermanentError, LLMTransientError
from ecet.application.ports.llm_gateway import EvaluationOutput, EvaluationRequest
from ecet.application.prompts.evaluate_v1 import TOOL, TOOL_NAME, build_messages
from ecet.domain.evaluation import Evaluation

log = structlog.get_logger(__name__)

MAX_TOKENS = 1024

#: A retry would send the identical request and get the identical answer.
PERMANENT_STATUS = frozenset({400, 401, 403, 404})


class OpenAiLlmGateway:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_s: int,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._model = model
        self._client = AsyncOpenAI(
            base_url=base_url,
            api_key=api_key,
            timeout=float(timeout_s),
            max_retries=0,
            http_client=http_client,
        )

    async def aclose(self) -> None:
        await self._client.close()

    async def evaluate(self, request: EvaluationRequest) -> Evaluation:
        messages = cast(list[ChatCompletionMessageParam], build_messages(request))
        started = time.perf_counter()
        try:
            completion = await self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                tools=[cast(Any, TOOL)],
                tool_choice=cast(Any, {"type": "function", "function": {"name": TOOL_NAME}}),
                max_tokens=MAX_TOKENS,
                temperature=0,
            )
        except (APITimeoutError, APIConnectionError, RateLimitError, InternalServerError) as error:
            raise LLMTransientError(f"{type(error).__name__}: {error}") from error
        except APIStatusError as error:
            if error.status_code in PERMANENT_STATUS:
                raise LLMPermanentError(f"status {error.status_code}") from error
            raise LLMTransientError(f"status {error.status_code}") from error
        latency_ms = int((time.perf_counter() - started) * 1000)

        output = _parse(completion)
        usage = completion.usage
        evaluation = output.to_evaluation(
            model=completion.model or self._model,
            prompt_version=request.prompt_version,
            latency_ms=latency_ms,
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
            known_policy_ids=[policy.id for policy in request.policies],
        )
        log.info(
            "llm.evaluated",
            claim_id=str(request.claim_id),
            provider="openai",
            model=evaluation.model,
            latency_ms=evaluation.latency_ms,
            input_tokens=evaluation.input_tokens,
            output_tokens=evaluation.output_tokens,
            decision=evaluation.decision.value,
            confidence=evaluation.confidence,
        )
        return evaluation


def _parse(completion: Any) -> EvaluationOutput:
    """Every "the server answered, but not usefully" case collapses to one error type,
    because UC-06 treats them identically: the claim fails and a human looks at it."""
    choices = completion.choices or []
    tool_calls = getattr(choices[0].message, "tool_calls", None) if choices else None
    if not tool_calls:
        raise LLMInvalidOutput("the response contained no tool call")
    try:
        return EvaluationOutput.model_validate_json(tool_calls[0].function.arguments)
    except ValidationError as error:
        # The arguments may echo note content; only the error class is logged.
        raise LLMInvalidOutput(
            f"tool arguments rejected: {error.error_count()} problem(s)"
        ) from error
