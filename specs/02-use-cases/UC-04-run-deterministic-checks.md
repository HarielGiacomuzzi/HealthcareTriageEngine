# UC-04 RunDeterministicChecks ([ADR-002](../00-overview.md#4-adrs))

## Input / Output

`(redacted: RedactedText, policies: list[Policy])` → `DeterministicResult`.

## Steps

1. Extract candidate ICD-10 codes from text with `Icd10Code` regex (word-bounded, case-insensitive).
2. Run rules from `domain/rules.py` (see [`01-domain/evaluation.md` §1](../01-domain/evaluation.md#1-deterministicresult-adr-002)) in order.
3. Aggregate verdict.

Pure function wrapper; no ports. Exists as use case only for uniform wiring + metrics hook
(`deterministic_verdict_total{verdict}` counter).

## Cost note

Expected that some percentage of claims to be short-circuited ([README ADR-002](../../README.md#key-architecture-decisions-adrs)). Track ratio as metric
`llm_calls_avoided_total`.

## Tests

- Delegates to domain rule tests; add one integration-style test with realistic note fixtures per verdict.
