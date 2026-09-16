from prometheus_client import REGISTRY
from tests.fakes import FakePiiRedactor
from tests.pii import assert_no_pii, read_note

from ecet.application.use_cases.redact_pii import RedactPii


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


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


async def test_redaction_counts_its_entities_and_its_time() -> None:
    before_person = sample("ecet_pii_entities_total", entity="PERSON")
    before_count = sample("ecet_pii_redaction_seconds_count")

    await RedactPii(FakePiiRedactor()).execute(read_note("meets"))

    assert sample("ecet_pii_entities_total", entity="PERSON") > before_person
    assert sample("ecet_pii_redaction_seconds_count") == before_count + 1
