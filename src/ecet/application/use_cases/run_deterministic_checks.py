"""UC-04 RunDeterministicChecks (ADR-002).

A wrapper with no ports: the rules are pure and live in `domain/rules.py`. It is a
use case anyway so UC-01 wires all five sub use cases the same way, and so Phase 6
has one place to increment `deterministic_verdict_total` and `llm_calls_avoided_total`.
"""

from collections.abc import Sequence

from ecet.domain.claim import RedactedText
from ecet.domain.evaluation import DeterministicResult
from ecet.domain.policy import Policy
from ecet.domain.rules import run_checks


class RunDeterministicChecks:
    async def execute(
        self, redacted: RedactedText, policies: Sequence[Policy]
    ) -> DeterministicResult:
        # `async` with nothing to await: the uniform `await use_case.execute(...)` call
        # shape in UC-01 is worth more than saving this frame.
        return run_checks(redacted, policies)
