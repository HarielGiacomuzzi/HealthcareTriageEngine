"""`spacy download en_core_web_lg` resolves whatever release is newest for the
installed spaCy — unpinned, so two builds a week apart can redact differently. The
version is pinned in three files; this keeps them from drifting apart."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FILES = ("Dockerfile", "Makefile", ".github/workflows/ci.yml")
#: `ARG SPACY_MODEL_VERSION=3.8.0`, `SPACY_MODEL_VERSION ?= 3.8.0`, `SPACY_MODEL_VERSION: "3.8.0"`.
PIN = re.compile(r"SPACY_MODEL_VERSION\s*(?:\?=|=|:)\s*\"?([0-9]+\.[0-9]+\.[0-9]+)")


def pinned_in(relative: str) -> set[str]:
    return set(PIN.findall((ROOT / relative).read_text(encoding="utf-8")))


def test_the_model_version_is_pinned_and_agrees_everywhere() -> None:
    assert [pinned_in(relative) for relative in FILES] == [{"3.8.0"}] * len(FILES)


def test_no_unpinned_download_remains() -> None:
    for relative in FILES:
        content = (ROOT / relative).read_text(encoding="utf-8")
        assert "spacy download en_core_web_lg" not in content, relative
