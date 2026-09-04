# Infra: LLM Gateway ([ADR-004](../00-overview.md#4-adrs))

Port: `ecet/application/ports/llm_gateway.py`
```python
class LLMGateway(Protocol):
    async def evaluate(self, req: EvaluationRequest) -> Evaluation: ...
```
Errors (application-owned): `LLMTransientError`, `LLMPermanentError`, `LLMInvalidOutput`.

## Adapters (`ecet/infrastructure/llm/`)

### `openai_gateway.py` (default real vendor)
Generic OpenAI-compatible client — any server speaking `/v1/chat/completions` with
function calling: Anthropic's OpenAI-compat endpoint, OpenAI, Azure, vLLM, Ollama,
OpenRouter, LM Studio. No vendor branch in the adapter.
- `openai.AsyncOpenAI(base_url=ECET_LLM_BASE_URL, api_key=ECET_LLM_API_KEY, timeout=ECET_LLM_TIMEOUT_S)`,
  model `ECET_LLM_MODEL` (default `claude-sonnet-5`).
- Prompt rendered by application (`application/prompts/evaluate_v1.py`, see [UC-06 prompt](../02-use-cases/UC-06-evaluate-claim.md#prompt-application-owned-applicationpromptsevaluate_v1py)); adapter only transports.
- Structured output: one tool
  `tools=[{"type":"function","function":{"name":"submit_evaluation","parameters":<schema>}}]`
  where `<schema>` = JSON schema of `EvaluationOutput` (Pydantic subset of
  [`Evaluation`](../01-domain/evaluation.md#2-evaluation-llm-output-vendor-neutral)), forced with
  `tool_choice={"type":"function","function":{"name":"submit_evaluation"}}`.
  Guarantees JSON, no prose parsing.
- `max_tokens=1024`, `temperature=0`.
- Map: 429 / 5xx / timeout / connection error → `LLMTransientError`; 400 / 401 / 403 / 404 → `LLMPermanentError`;
  no `tool_calls` in response, unparseable `arguments`, or schema validation failure → `LLMInvalidOutput`.
- Fill `model`, `latency_ms`; `input_tokens` / `output_tokens` from `usage.prompt_tokens` /
  `usage.completion_tokens`, defaulting to 0 — local servers often omit `usage`.
- Redact nothing here — input already redacted ([ADR-001](../00-overview.md#4-adrs)). Trust boundary is
  [UC-02](../02-use-cases/UC-02-redact-pii.md); adapter just sends.

<!-- ponytail: no JSON-mode/response_format fallback for servers lacking tool support. Add when a target server actually fails the tool path. -->

### `fake_gateway.py`
- Deterministic: returns canned `Evaluation` keyed by substring rules
  (`"denied"` in text → DOES_NOT_MEET 0.92; `"unclear"` → INSUFFICIENT 0.4; else MEETS 0.91).
- Selected by `ECET_LLM_PROVIDER=fake`. Default in compose so demo runs with **no API key**.
  Real vendor requires `ECET_LLM_PROVIDER=openai` + `ECET_LLM_BASE_URL` + `ECET_LLM_API_KEY`.

## Selection
`infrastructure/llm/factory.py: build_gateway(settings) -> LLMGateway` — `match settings.llm_provider`.

## Cost / observability
- Counter `llm_calls_total{provider,model,outcome}`, histogram `llm_latency_seconds`,
  counters `llm_tokens_total{direction}`.
- Log per call: claim_id, model, latency, tokens, decision, confidence. Never prompt body.

## Tests
- OpenAI adapter: `respx`/mock transport fixtures against `base_url` for success, 429,
  malformed tool `arguments`, missing `tool_calls`, missing `usage`.
- Fake adapter rule table.
