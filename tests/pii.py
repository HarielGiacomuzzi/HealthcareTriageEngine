"""The privacy assertion the testing spec makes mandatory for every layer that
touches text (ADR-001).

All strings here are synthetic. They appear verbatim in every note fixture, so any
string that survives redaction shows up as a failing assertion rather than as a
production incident.
"""

from pathlib import Path

NOTES_DIR = Path(__file__).resolve().parent / "fixtures" / "notes"

#: Every needle that must never appear downstream of redaction. `Whitfield` is listed
#: separately from the full name so a partial match still fails the assertion.
PII_STRINGS: tuple[str, ...] = (
    "Marcus Whitfield",
    "Whitfield",
    "Alicia Ferreira",
    "(415) 555-0137",
    "m.whitfield@example.com",
    "123-45-6789",
    "MRN: 4482910",
    "NWH2288341",
)


def read_note(name: str) -> str:
    """Read `tests/fixtures/notes/<name>.txt`."""
    return (NOTES_DIR / f"{name}.txt").read_text(encoding="utf-8")


def assert_no_pii(value: str) -> None:
    leaked = sorted({needle for needle in PII_STRINGS if needle in value})
    assert not leaked, f"PII leaked into the value under test: {leaked}"
