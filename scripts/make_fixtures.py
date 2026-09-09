"""Generate the PDF fixtures from the note fixtures.

Reproducible instead of committed: a binary in git is a binary nobody can review, and
the text inside these ones is the thing under test. `tests/fixtures/pdfs/` is
gitignored; the test session regenerates anything missing.

Run directly: `make fixtures`.
"""

from pathlib import Path

from reportlab.lib.pagesizes import LETTER
from reportlab.lib.pdfencrypt import StandardEncryption
from reportlab.pdfgen import canvas

REPO_ROOT = Path(__file__).resolve().parents[1]
NOTES_DIR = REPO_ROOT / "tests" / "fixtures" / "notes"
PDFS_DIR = REPO_ROOT / "tests" / "fixtures" / "pdfs"

LEFT_MARGIN = 72
TOP_MARGIN = 720
LINE_HEIGHT = 14


def _write_lines(target: canvas.Canvas, lines: list[str]) -> None:
    y = TOP_MARGIN
    for line in lines:
        if y < LEFT_MARGIN:
            target.showPage()
            y = TOP_MARGIN
        target.setFont("Helvetica", 10)
        target.drawString(LEFT_MARGIN, y, line[:110])
        y -= LINE_HEIGHT


def _note_lines(name: str) -> list[str]:
    return (NOTES_DIR / f"{name}.txt").read_text(encoding="utf-8").splitlines()


def build_all(destination: Path) -> dict[str, Path]:
    destination.mkdir(parents=True, exist_ok=True)
    built: dict[str, Path] = {}

    simple = destination / "note_simple.pdf"
    target = canvas.Canvas(str(simple), pagesize=LETTER)
    _write_lines(target, _note_lines("meets"))
    target.save()
    built["note_simple"] = simple

    unclear = destination / "note_unclear.pdf"
    target = canvas.Canvas(str(unclear), pagesize=LETTER)
    _write_lines(target, _note_lines("unclear"))
    target.save()
    built["note_unclear"] = unclear

    multipage = destination / "note_multipage.pdf"
    target = canvas.Canvas(str(multipage), pagesize=LETTER)
    for note in ("meets", "does_not_meet", "unclear"):
        _write_lines(target, _note_lines(note))
        target.showPage()
    target.save()
    built["note_multipage"] = multipage

    # No text operators at all — the stand-in for a scanned page. OCR is out of scope
    # for v1, so this must fail extraction rather than half-succeed.
    scanned = destination / "scanned.pdf"
    target = canvas.Canvas(str(scanned), pagesize=LETTER)
    target.rect(100, 100, 400, 500, stroke=1, fill=0)
    target.save()
    built["scanned"] = scanned

    encrypted = destination / "encrypted.pdf"
    target = canvas.Canvas(
        str(encrypted),
        pagesize=LETTER,
        encrypt=StandardEncryption(userPassword="user-secret", ownerPassword="owner-secret"),
    )
    _write_lines(target, _note_lines("meets"))
    target.save()
    built["encrypted"] = encrypted

    return built


if __name__ == "__main__":
    for name, path in build_all(PDFS_DIR).items():
        print(f"{name}: {path.relative_to(REPO_ROOT)}")
