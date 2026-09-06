"""Which entities are redacted and what replaces them (UC-02).

Application-owned, not adapter-owned: the presidio adapter receives this at
construction. Changing redaction behaviour is a policy change, not an infrastructure
change.
"""

#: Entity label -> replacement token. The adapter analyses for exactly these labels.
ENTITY_REPLACEMENTS: dict[str, str] = {
    "PERSON": "<PERSON>",
    "PHONE_NUMBER": "<PHONE>",
    "EMAIL_ADDRESS": "<EMAIL>",
    "US_SSN": "<SSN>",
    "LOCATION": "<LOCATION>",
    "MEDICAL_LICENSE": "<LICENSE>",
    "US_DRIVER_LICENSE": "<ID>",
    "CREDIT_CARD": "<ID>",
    "IBAN_CODE": "<ID>",
    "IP_ADDRESS": "<ID>",
    "MRN": "<MRN>",
    "MEMBER_ID": "<MEMBER_ID>",
}

#: Detected but deliberately left in place: dates of service drive policy evaluation.
KEPT_ENTITIES: frozenset[str] = frozenset({"DATE_TIME"})

#: Custom pattern recognisers registered into presidio's registry.
CUSTOM_PATTERNS: dict[str, str] = {
    "MRN": r"\bMRN[:# ]*\d{6,10}\b",
    "MEMBER_ID": r"\b[A-Z]{2,3}\d{7,10}\b",
}

#: Presidio's default. Below this, a detection is discarded.
SCORE_THRESHOLD = 0.35
