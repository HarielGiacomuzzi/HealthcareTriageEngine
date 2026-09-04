# Domain: Evaluation & Triage Decision

Module: `ecet/domain/evaluation.py`.

## 1. `DeterministicResult` ([ADR-002](../00-overview.md#4-adrs))

Output of rule checks run **before** LLM.

| Field     | Type                        |
|-----------|-----------------------------|
| verdict   | `PASS` \| `REJECT` \| `UNCERTAIN` |
| checks    | list[CheckOutcome]          |

`CheckOutcome`: `name: str`, `passed: bool`, `detail: str`.

Rules (v1, in `domain/rules.py`, pure functions over [`RedactedText`](claim.md#redactedtext-value-object-frozen) + [`list[Policy]`](policy.md#2-entity-policy)):

| Rule                | Passes when                                                       | Fails when — and what that means                                                                                                     | Verdict on fail |
|---------------------|-------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|-----------------|
| `non_empty_text`    | Redacted note has ≥ 50 characters.                                | Under 50 characters left after redaction: the document is effectively empty (blank scan, image-only PDF, or all content was PII). Nothing for the LLM to read. | REJECT          |
| `icd10_present`     | At least one ICD-10 pattern matches in the note.                  | No diagnosis code anywhere in the note. Cannot tell which condition is being claimed, so coverage cannot be checked mechanically — a human must read it. | UNCERTAIN       |
| `excluded_code_hit` | None of the found codes appear in any policy's `excluded_codes`.  | A found code is explicitly excluded by one of the tenant's policies. The policy already says this is not covered; no LLM call would change that. | REJECT          |
| `covered_code_hit`  | At least one found code appears in some policy's `covered_codes`. | None of the found codes are listed as covered by any active policy. Not proof of denial — the policy set may simply not enumerate this code — so it needs judgment, not an automatic no. | UNCERTAIN       |

Aggregation: any REJECT → REJECT; else any UNCERTAIN → UNCERTAIN; else PASS.
Only PASS and UNCERTAIN proceed to LLM. REJECT → human review (never auto-deny).

## 2. `Evaluation` (LLM output, vendor-neutral)

| Field            | Type                    | Notes |
|------------------|-------------------------|-------|
| decision         | `MEETS_NECESSITY` \| `DOES_NOT_MEET` \| `INSUFFICIENT_EVIDENCE` | |
| confidence       | float, 0.0–1.0          | vendor must supply; missing → INSUFFICIENT_EVIDENCE, 0.0 |
| matched_policy_id| PolicyId \| None        |       |
| cited_codes      | list[Icd10Code]         |       |
| rationale        | str                     | ≤ 2000 chars |
| evidence_found   | list[str]               | subset of [policy](policy.md#2-entity-policy) `required_evidence` |
| evidence_missing | list[str]               |       |
| model            | str                     | e.g. `claude-sonnet-5` (value of `ECET_LLM_MODEL`) |
| prompt_version   | str                     | for reproducibility |
| latency_ms       | int                     |       |
| input_tokens     | int                     |       |
| output_tokens    | int                     |       |

Strict schema: vendor JSON parsed with `Evaluation.model_validate`; validation
error → `EVALUATION_FAILED` with reason, message nacked to DLQ.

## 3. `TriageDecision` (pure function, [ADR-003](../00-overview.md#4-adrs))

```python
def triage(evaluation: Evaluation, threshold: float) -> Route
# Route = AUTO_NOTIFY | HUMAN_REVIEW
```

- `confidence >= threshold` and `decision != INSUFFICIENT_EVIDENCE` → `AUTO_NOTIFY`.
- Otherwise → `HUMAN_REVIEW`.

## 4. `ReviewTask` (human-in-the-loop)

| Field        | Type                 |
|--------------|----------------------|
| id           | UUID                 |
| claim_id     | ClaimId              |
| tenant_id    | TenantId             |
| reason       | `LOW_CONFIDENCE` \| `DETERMINISTIC_REJECT` \| `DETERMINISTIC_UNCERTAIN_LLM_LOW` \| `EVALUATION_FAILED` |
| status       | `OPEN` \| `RESOLVED` |
| resolution   | `MEETS_NECESSITY` \| `DOES_NOT_MEET` \| None |
| reviewer     | str \| None          |
| notes        | str \| None          |
| created_at / resolved_at | datetime |

Port `ReviewTaskRepository`: `add`, `get`, `list_open(tenant_id, limit)`, `save`.

## 5. Tests
- Rule table (each rule × pass/fail).
- Aggregation precedence.
- `triage` boundary: 0.85 exactly → AUTO_NOTIFY; 0.8499 → HUMAN_REVIEW; INSUFFICIENT_EVIDENCE at 0.99 → HUMAN_REVIEW.
- `Evaluation` rejects confidence outside [0,1].
