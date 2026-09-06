"""Deterministic pre-LLM checks (ADR-002).

Cheap, pure rules over the redacted note and the tenant's policies. A REJECT here
means no LLM call is made at all — that skipped call is the cost saving the ADR is
about. REJECT never means an automatic denial: it routes to a human.
"""

import re
from collections.abc import Sequence

from ecet.domain.claim import RedactedText
from ecet.domain.evaluation import CheckOutcome, DeterministicResult, Verdict
from ecet.domain.policy import Icd10Code, Policy

MIN_TEXT_CHARS = 50

#: Word-bounded, case-insensitive ICD-10 token. Same shape as `ICD10_PATTERN`,
#: unanchored so it can be searched inside prose.
# ponytail: pattern-only match, no code dictionary — "Vitamin B12" and the "T12" vertebra
# both read as codes. Validate against seeded ICD-10 codes once Phase 2 lands them.
ICD10_TOKEN_RE = re.compile(r"\b[A-TV-Z][0-9][0-9A-Z](?:\.[0-9A-Z]{1,4})?\b", re.IGNORECASE)

#: What a failing rule means for the overall verdict.
_VERDICT_ON_FAIL = {
    "non_empty_text": Verdict.REJECT,
    "icd10_present": Verdict.UNCERTAIN,
    "excluded_code_hit": Verdict.REJECT,
    "covered_code_hit": Verdict.UNCERTAIN,
}


def extract_icd10_codes(text: str) -> list[Icd10Code]:
    """Codes found in the note, normalised, de-duplicated, in first-seen order."""
    found: dict[str, Icd10Code] = {}
    for match in ICD10_TOKEN_RE.finditer(text):
        parsed = Icd10Code(code=match.group())
        found.setdefault(parsed.code, parsed)
    return list(found.values())


def aggregate(checks: Sequence[CheckOutcome]) -> Verdict:
    """Any REJECT wins; otherwise any UNCERTAIN; otherwise PASS."""
    failures = {_VERDICT_ON_FAIL[check.name] for check in checks if not check.passed}
    if Verdict.REJECT in failures:
        return Verdict.REJECT
    if Verdict.UNCERTAIN in failures:
        return Verdict.UNCERTAIN
    return Verdict.PASS


def run_checks(redacted: RedactedText, policies: Sequence[Policy]) -> DeterministicResult:
    """Run every rule, always, then aggregate. All four outcomes are reported so the
    claim record shows what was checked, not only what failed."""
    codes = extract_icd10_codes(redacted.text)
    covered = {code for policy in policies for code in policy.covered_codes}
    excluded = {code for policy in policies for code in policy.excluded_codes}

    checks = [
        _non_empty_text(redacted),
        _icd10_present(codes),
        _excluded_code_hit(codes, excluded),
        _covered_code_hit(codes, covered),
    ]
    return DeterministicResult(verdict=aggregate(checks), checks=checks)


def _non_empty_text(redacted: RedactedText) -> CheckOutcome:
    length = len(redacted.text.strip())
    return CheckOutcome(
        name="non_empty_text",
        passed=length >= MIN_TEXT_CHARS,
        detail=f"{length} characters after redaction (minimum {MIN_TEXT_CHARS})",
    )


def _icd10_present(codes: Sequence[Icd10Code]) -> CheckOutcome:
    return CheckOutcome(
        name="icd10_present",
        passed=bool(codes),
        detail=f"{len(codes)} ICD-10 code(s) found",
    )


def _excluded_code_hit(codes: Sequence[Icd10Code], excluded: set[Icd10Code]) -> CheckOutcome:
    hits = sorted(code.code for code in codes if code in excluded)
    return CheckOutcome(
        name="excluded_code_hit",
        passed=not hits,
        detail=f"excluded codes present: {', '.join(hits)}" if hits else "no excluded code found",
    )


def _covered_code_hit(codes: Sequence[Icd10Code], covered: set[Icd10Code]) -> CheckOutcome:
    hits = sorted(code.code for code in codes if code in covered)
    return CheckOutcome(
        name="covered_code_hit",
        passed=bool(hits),
        detail=f"covered codes present: {', '.join(hits)}" if hits else "no covered code found",
    )
