"""The pypdf adapter needs no container, so it stays in the default run rather than
living under `tests/adapters/` where the whole directory is marked `slow`."""

import time
from io import BytesIO
from pathlib import Path

import pytest
from prometheus_client import REGISTRY
from pypdf import PdfReader
from scripts.make_fixtures import build_all

from ecet.application.errors import ExtractionFailed
from ecet.infrastructure.pdf.pypdf_extractor import PypdfTextExtractor

PDFS_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "pdfs"


@pytest.fixture(scope="session")
def pdfs() -> dict[str, Path]:
    """Regenerate on demand — `tests/fixtures/pdfs/` is gitignored."""
    return build_all(PDFS_DIR)


def read(pdfs: dict[str, Path], name: str) -> bytes:
    return pdfs[name].read_bytes()


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


async def test_a_simple_note_extracts_its_text(pdfs: dict[str, Path]) -> None:
    text = await PypdfTextExtractor(max_pages=50).extract(read(pdfs, "note_simple"))

    assert "M54.5" in text
    assert "Marcus Whitfield" in text  # raw text is raw — redaction happens later


async def test_pages_are_joined_with_a_blank_line(pdfs: dict[str, Path]) -> None:
    text = await PypdfTextExtractor(max_pages=50).extract(read(pdfs, "note_multipage"))

    assert "\n\n" in text
    assert text.count("Prior Authorization Request") == 3


async def test_a_scanned_page_fails_with_no_text(pdfs: dict[str, Path]) -> None:
    with pytest.raises(ExtractionFailed, match="no_text"):
        await PypdfTextExtractor(max_pages=50).extract(read(pdfs, "scanned"))


async def test_an_encrypted_pdf_fails(pdfs: dict[str, Path]) -> None:
    with pytest.raises(ExtractionFailed, match="encrypted"):
        await PypdfTextExtractor(max_pages=50).extract(read(pdfs, "encrypted"))


async def test_the_page_cap_is_enforced(pdfs: dict[str, Path]) -> None:
    with pytest.raises(ExtractionFailed, match="too_many_pages"):
        await PypdfTextExtractor(max_pages=1).extract(read(pdfs, "note_multipage"))


async def test_garbage_bytes_fail_as_unreadable() -> None:
    with pytest.raises(ExtractionFailed, match="unreadable"):
        await PypdfTextExtractor(max_pages=50).extract(b"not a pdf at all")


async def test_the_normaliser_collapses_runs_of_blank_lines() -> None:
    from ecet.infrastructure.pdf.pypdf_extractor import normalise

    assert normalise("a   \n\n\n\n\nb  \n") == "a\n\nb"


async def test_extraction_is_timed(pdfs: dict[str, Path]) -> None:
    before = sample("ecet_pdf_extract_seconds_count")

    await PypdfTextExtractor(max_pages=50).extract(read(pdfs, "note_simple"))

    assert sample("ecet_pdf_extract_seconds_count") == before + 1


async def test_a_five_page_note_extracts_within_the_soft_budget(pdfs: dict[str, Path]) -> None:
    """pdf-text-extractor spec: a 5-page fixture extracts in under 200 ms. Best of three,
    so a cold import or a noisy CI neighbour does not fail the build — a real regression
    is slow every time."""
    data = read(pdfs, "note_five_pages")
    assert len(PdfReader(BytesIO(data)).pages) == 5
    extractor = PypdfTextExtractor(max_pages=50)

    timings: list[float] = []
    for _ in range(3):
        started = time.perf_counter()
        await extractor.extract(data)
        timings.append(time.perf_counter() - started)

    assert min(timings) < 0.2, f"best of three took {min(timings) * 1000:.0f} ms"
