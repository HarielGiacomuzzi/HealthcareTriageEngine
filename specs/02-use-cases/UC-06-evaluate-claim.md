# UC-06 EvaluateClaim

Worker-side orchestrator. One message → one LLM call → route.

## Input
`EvaluationMessage`.

## Dependencies
`UnitOfWork`, `LLMGateway`, `RouteDecision` ([UC-07](UC-07-route-decision.md)), `Clock`.

## Steps
1. Load claim. Not found → ack + log error (poison message).
   Status not in `{QUEUED}` → ack, log `skipped_duplicate` (at-least-once delivery; [ADR-006](../00-overview.md#4-adrs)).
2. Build `EvaluationRequest`:
   ```python
   class EvaluationRequest(BaseModel):
       claim_id: ClaimId
       redacted_text: str
       policies: list[PolicySnapshot]
       found_codes: list[str]
       prompt_version: str      # from config, e.g. "v1"
   ```
3. `llm.evaluate(req)` with timeout (config `llm_timeout_s`, default 60).
   - `LLMTransientError` (429/5xx/timeout) → raise; consumer nacks with requeue, up to `max_retries` (header `x-retry-count`), then DLQ.
   - `LLMInvalidOutput` / `LLMPermanentError` → claim `EVALUATION_FAILED` + reason, [UC-09](UC-09-human-review.md) review task reason `EVALUATION_FAILED`, ack.
4. Store `claim.evaluation`, transition `EVALUATED`, save, commit.
5. UC-07 RouteDecision.

## Prompt (application-owned, `application/prompts/evaluate_v1.py`)
- System: role = utilisation review assistant; output **only** JSON matching `Evaluation` subset
  (`decision, confidence, matched_policy_id, cited_codes, rationale, evidence_found, evidence_missing`).
- User: redacted note + policies rendered as numbered blocks + found codes.
- Confidence instruction: calibrated probability that decision is correct; unknown → low.
- Prompt text versioned; `prompt_version` stored on `Evaluation`.

Vendor adapter fills `model`, `latency_ms`, tokens. Application validates JSON.

## Tests
- Fake gateway returning fixed `Evaluation` → claim EVALUATED, route called.
- Gateway raises transient → exception propagates, claim untouched.
- Gateway returns invalid JSON → EVALUATION_FAILED + review task.
- Redelivered message for already-EVALUATED claim → no gateway call.
