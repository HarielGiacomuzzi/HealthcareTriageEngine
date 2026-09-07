from tests.fakes import FakePiiRedactor
from tests.pii import assert_no_pii, read_note

from ecet.application.use_cases.redact_pii import RedactPii


async def test_it_returns_the_ports_redacted_text() -> None:
    use_case = RedactPii(FakePiiRedactor())

    result = await use_case.execute(read_note("meets"))

    assert_no_pii(result.text)
    assert result.entity_counts["US_SSN"] == 1
    assert result.redactor == "fake"


async def test_empty_text_is_not_an_error() -> None:
    result = await RedactPii(FakePiiRedactor()).execute("")

    assert result.text == ""
    assert result.entity_counts == {}


async def test_icd10_codes_survive_redaction() -> None:
    result = await RedactPii(FakePiiRedactor()).execute(read_note("meets"))

    assert "M54.5" in result.text
