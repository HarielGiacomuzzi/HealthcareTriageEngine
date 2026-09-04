# UC-02 RedactPii

Thin use case over `PiiRedactor` port. Exists so redaction policy (which entities,
which operators) is application-owned, not adapter-owned ([ADR-001](../00-overview.md#4-adrs)).

## Input / Output

`text: str` → `RedactedText`.

## Redaction Policy (application constant, `application/redaction_policy.py`)

| Entity                                                   | Operator | Replacement                                        |
| -------------------------------------------------------- | -------- | -------------------------------------------------- |
| PERSON                                                   | replace  | `<PERSON>`                                         |
| PHONE_NUMBER                                             | replace  | `<PHONE>`                                          |
| EMAIL_ADDRESS                                            | replace  | `<EMAIL>`                                          |
| US_SSN                                                   | replace  | `<SSN>`                                            |
| LOCATION                                                 | replace  | `<LOCATION>`                                       |
| DATE_TIME                                                | keep     | Dates of service needed for policy evaluation      |
| MEDICAL_LICENSE                                          | replace  | `<LICENSE>`                                        |
| US_DRIVER_LICENSE / CREDIT_CARD / IBAN_CODE / IP_ADDRESS | replace  | `<ID>`                                             |
| Custom `MRN`                                             | replace  | `<MRN>` (regex recogniser `\bMRN[:# ]*\d{6,10}\b`) |
| Custom `MEMBER_ID`                                       | replace  | `<MEMBER_ID>` (regex `\b[A-Z]{2,3}\d{7,10}\b`)     |

Score threshold: 0.35 (presidio default). ICD-10 codes must **not** be redacted;
add allow-list deny recogniser test (`M54.5`, `E11.9` survive).

## Rules

- Input > 200 k chars → chunk by paragraph, redact per chunk, join. Adapter concern; use case just calls port.
- `entity_counts` aggregated and stored for audit/metrics.
- Never log input text. Log only counts + duration.

## Tests

- Fixture note with name, phone, SSN, MRN, ICD-10 codes → all PII placeholders present, codes intact.
- Empty string → empty `RedactedText`, no error.
- Uses `FakePiiRedactor` in use-case tests; real presidio only in adapter tests.
