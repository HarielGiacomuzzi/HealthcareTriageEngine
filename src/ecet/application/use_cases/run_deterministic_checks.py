"""UC-04 RunDeterministicChecks (ADR-002).

A wrapper with no ports: the rules are pure and live in `domain/rules.py`. It is a
use case anyway so UC-01 wires all five sub use cases the same way.
"""

from collections.abc import Sequence

from ecet import metrics
from ecet.domain.claim import RedactedText
from ecet.domain.evaluation import DeterministicResult, Verdict
from ecet.domain.policy import Policy
from ecet.domain.rules import run_checks


class RunDeterministicChecks:
    async def execute(
        self, redacted: RedactedText, policies: Sequence[Policy]
    ) -> DeterministicResult:
        # `async` with nothing to await: the uniform `await use_case.execute(...)` call
        # shape in UC-01 is worth more than saving this frame.
        result = run_checks(redacted, policies)
        metrics.DETERMINISTIC_VERDICT_TOTAL.labels(verdict=result.verdict.value).inc()
        if result.verdict is Verdict.REJECT:
            # ADR-002's saving, counted: this claim goes to a human and the vendor is
            # never called.
            metrics.LLM_CALLS_AVOIDED_TOTAL.inc()
        return result
