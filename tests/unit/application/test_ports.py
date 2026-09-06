"""Every fake must satisfy the Protocol it stands in for — otherwise a use case
tested against fakes proves nothing about the adapter that replaces them."""

from datetime import UTC, datetime

from tests.fakes import FakeObjectStorage, FakePiiRedactor, FakeTextExtractor, FixedClock

from ecet.application.ports.clock import Clock
from ecet.application.ports.object_storage import ObjectStorage
from ecet.application.ports.pii_redactor import PiiRedactor
from ecet.application.ports.text_extractor import TextExtractor


def test_fixed_clock_is_a_clock() -> None:
    assert isinstance(FixedClock(datetime(2026, 9, 6, tzinfo=UTC)), Clock)


def test_fake_object_storage_is_an_object_storage() -> None:
    assert isinstance(FakeObjectStorage(), ObjectStorage)


def test_fake_text_extractor_is_a_text_extractor() -> None:
    assert isinstance(FakeTextExtractor(), TextExtractor)


def test_fake_pii_redactor_is_a_pii_redactor() -> None:
    assert isinstance(FakePiiRedactor(), PiiRedactor)
