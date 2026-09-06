import pytest
from tests.fakes import FakePiiRedactor
from tests.pii import PII_STRINGS, assert_no_pii, read_note


def test_assert_no_pii_accepts_clean_text() -> None:
    assert_no_pii("Patient <PERSON> reports low back pain, M54.5.")


def test_assert_no_pii_rejects_a_leaked_string() -> None:
    with pytest.raises(AssertionError, match="Marcus Whitfield"):
        assert_no_pii("Patient Marcus Whitfield reports low back pain.")


@pytest.mark.parametrize("name", ["meets", "does_not_meet", "unclear", "excluded_code", "no_codes"])
def test_every_note_fixture_carries_the_pii_block(name: str) -> None:
    note = read_note(name)
    assert all(needle in note for needle in PII_STRINGS)


async def test_the_fake_redactor_clears_every_note() -> None:
    redactor = FakePiiRedactor()
    for name in ("meets", "does_not_meet", "unclear", "excluded_code", "no_codes"):
        assert_no_pii((await redactor.redact(read_note(name))).text)


async def test_the_fake_redactor_leaves_icd10_codes_alone() -> None:
    result = await FakePiiRedactor().redact(read_note("meets"))

    assert "M54.5" in result.text
