# Infra: PII Redactor (Presidio 2.2.x)

Module: `ecet/infrastructure/pii/presidio_redactor.py`. Implements `PiiRedactor`.

## Adapter
- `AnalyzerEngine` with `NlpEngineProvider` config: spaCy `en_core_web_lg`. Built **once** at
  process start (model load ~2–4 s, ~600 MB RAM). Singleton in container wiring, not per request.
- Custom `PatternRecognizer`s for `MRN`, `MEMBER_ID` (patterns in [UC-02](../02-use-cases/UC-02-redact-pii.md)) registered into recogniser registry.
- `AnonymizerEngine` with `OperatorConfig("replace", {"new_value": "<X>"})` per entity from application redaction policy ([UC-02 table](../02-use-cases/UC-02-redact-pii.md#redaction-policy-application-constant-applicationredaction_policypy)). Adapter receives policy dict at construction; does not hardcode.
- Chunking: split on `\n\n` into ≤ 20 k char chunks; analyze + anonymize each; join. Keeps spaCy memory bounded.
- Runs in `anyio.to_thread` with a semaphore (`ECET_PII_CONCURRENCY`, default 2) — spaCy not thread-safe-cheap.
- Returns `RedactedText(text, entity_counts, redactor="presidio-2.2")`.

## Dockerfile impact
- `pip install presidio-analyzer==2.2.* presidio-anonymizer==2.2.*` + `python -m spacy download en_core_web_lg` in build stage. Image ~1.5 GB; acceptable, note in README.

## Metrics
- `pii_redaction_seconds` histogram, `pii_entities_total{entity}` counter.

## Tests
- Real engine (marked `@pytest.mark.slow`): fixture note → placeholders present, ICD-10 codes untouched, `entity_counts` ≥ expected.
- Chunk boundary: entity spanning `\n\n` acceptable loss; document limitation.
- Unit tests elsewhere use `FakePiiRedactor` (regex-based, instant).
