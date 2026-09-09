"""Prompt v1 for UC-06.

The prompt lives in the application layer, not in the adapter, because it is part of
what the system decides — the adapter only transports it. `PROMPT_VERSION` is stored
on every `Evaluation`, so an output can always be traced back to the text that
produced it: change the wording, bump the version.

Structured output is a forced tool call rather than "reply in JSON", so there is no
prose to parse and no fenced-code stripping anywhere in the codebase.
"""

from typing import Any

from ecet.application.messages import PolicySnapshot
from ecet.application.ports.llm_gateway import EvaluationOutput, EvaluationRequest

__all__ = [
    "PROMPT_VERSION",
    "SYSTEM_PROMPT",
    "TOOL",
    "TOOL_NAME",
    "build_messages",
    "render_user_message",
]

PROMPT_VERSION = "v1"
TOOL_NAME = "submit_evaluation"

SYSTEM_PROMPT = """You are a utilisation review assistant for a health insurer.

You are given one de-identified clinical note and the payer policies that apply to \
the requested service. Decide whether the note establishes medical necessity under \
those policies.

Rules:
- Judge only against the policies given. Do not apply outside clinical knowledge as \
if it were policy.
- MEETS_NECESSITY: the note documents every criterion of at least one policy.
- DOES_NOT_MEET: the note contradicts a criterion, or the service is excluded.
- INSUFFICIENT_EVIDENCE: the note neither establishes nor contradicts the criteria.
- confidence is your calibrated probability that the decision is correct, from 0.0 to \
1.0. Always supply it. If you cannot tell, choose INSUFFICIENT_EVIDENCE with a low \
confidence rather than guessing a decision with a high one.
- matched_policy_id must be one of the policy ids given, or null.
- evidence_found and evidence_missing are drawn from the policies' required evidence.
- rationale is at most a short paragraph and cites what the note says.

The note has already been de-identified. Do not speculate about the identity of the \
patient or the provider, and do not ask for the removed values.

Answer only by calling the submit_evaluation tool."""

TOOL: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": TOOL_NAME,
        "description": "Submit the medical-necessity evaluation for this claim.",
        "parameters": EvaluationOutput.model_json_schema(),
    },
}


def _render_policy(index: int, policy: PolicySnapshot) -> str:
    covered = ", ".join(policy.covered_codes) or "none listed"
    excluded = ", ".join(policy.excluded_codes) or "none listed"
    evidence = "\n".join(f"    - {item}" for item in policy.required_evidence) or "    - none"
    return (
        f"Policy {index}\n"
        f"  id: {policy.id}\n"
        f"  name: {policy.name} (version {policy.version})\n"
        f"  covered codes: {covered}\n"
        f"  excluded codes: {excluded}\n"
        f"  criteria: {policy.criteria_text}\n"
        f"  required evidence:\n{evidence}"
    )


def render_user_message(request: EvaluationRequest) -> str:
    policies = "\n\n".join(
        _render_policy(index, policy) for index, policy in enumerate(request.policies, start=1)
    )
    codes = ", ".join(request.found_codes)
    return (
        "APPLICABLE POLICIES\n"
        f"{policies or 'none'}\n\n"
        "DIAGNOSIS CODES FOUND IN THE NOTE\n"
        f"{codes or 'none'}\n\n"
        "DE-IDENTIFIED CLINICAL NOTE\n"
        f"{request.redacted_text}"
    )


def build_messages(request: EvaluationRequest) -> list[dict[str, str]]:
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": render_user_message(request)},
    ]
