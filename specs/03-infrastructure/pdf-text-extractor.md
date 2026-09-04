# Infra: PDF Text Extractor

Module: `ecet/infrastructure/pdf/pypdf_extractor.py`. Implements [`TextExtractor`](../02-use-cases/README.md#ports-defined-in-ecetapplicationports).

## Adapter
- `pypdf.PdfReader(BytesIO(pdf))`, concat `page.extract_text()` with `\n\n` between pages.
- Runs in `anyio.to_thread` (CPU-bound, keep event loop free).
- Normalise: collapse >2 blank lines, strip trailing whitespace.
- Encrypted PDF → try empty password; still locked → `ExtractionFailed("encrypted")`.
- Result < 50 chars → `ExtractionFailed("no_text")` (likely scanned; OCR out of scope v1).
- Page cap: [`ECET_MAX_PDF_PAGES`](../05-platform/config.md) (default 50) → `ExtractionFailed("too_many_pages")`.

## Fixtures (`tests/fixtures/pdfs/`)
- `note_simple.pdf` (1 page, PII + ICD-10), `note_multipage.pdf`, `scanned.pdf` (image only), `encrypted.pdf`.
Generated via `reportlab` script `scripts/make_fixtures.py` so fixtures reproducible, no binaries committed... except `scanned.pdf` (tiny).

## Tests
- Each fixture → expected outcome.
- Extraction time for 5-page fixture < 200 ms (soft budget for 1.5 s goal).
