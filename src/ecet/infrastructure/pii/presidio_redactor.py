"""`PiiRedactor` over Presidio 2.2 + spaCy (ADR-001).

Three things this file is careful about:

**Cost.** `AnalyzerEngine` loads a ~600 MB spaCy model in 2-4 s. It is built once, at
process start, by the API container wiring — never per request.

**Concurrency.** spaCy is not cheap to run in parallel, so the CPU work goes to a
worker thread behind a semaphore sized by `ECET_PII_CONCURRENCY`.

**Policy.** Which entities are redacted, what replaces them, and the custom pattern
regexes are all passed in from `application/redaction_policy.py` — none of that is
hardcoded here, and `DATE_TIME` is simply absent from the analysed entity list, which
is how dates of service survive. Two things ARE adapter-owned rather than
application-owned: the ICD-10 span exemption (a presidio quirk — its NER tags bare
alphanumeric codes as locations — not a policy choice any other engine would need)
and the `score=0.9` given to every custom pattern recognizer (an engine wiring detail,
not a redaction rule).
"""

import time
from collections.abc import Mapping

import anyio
import anyio.to_thread
import structlog
from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer, RecognizerRegistry
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig

from ecet.domain.claim import RedactedText
from ecet.domain.rules import ICD10_TOKEN_RE

log = structlog.get_logger(__name__)

REDACTOR = "presidio-2.2"
DEFAULT_CHUNK_CHARS = 20_000


def _build_analyzer(spacy_model: str, custom_patterns: Mapping[str, str]) -> AnalyzerEngine:
    provider = NlpEngineProvider(
        nlp_configuration={
            "nlp_engine_name": "spacy",
            "models": [{"lang_code": "en", "model_name": spacy_model}],
        }
    )
    nlp_engine = provider.create_engine()
    registry = RecognizerRegistry()
    registry.load_predefined_recognizers(nlp_engine=nlp_engine, languages=["en"])
    for entity, regex in custom_patterns.items():
        registry.add_recognizer(
            PatternRecognizer(
                supported_entity=entity,
                patterns=[Pattern(name=f"{entity.lower()}_pattern", regex=regex, score=0.9)],
            )
        )
    return AnalyzerEngine(nlp_engine=nlp_engine, registry=registry)


class PresidioPiiRedactor:
    def __init__(
        self,
        *,
        replacements: Mapping[str, str],
        custom_patterns: Mapping[str, str],
        score_threshold: float,
        spacy_model: str,
        concurrency: int,
        chunk_chars: int = DEFAULT_CHUNK_CHARS,
    ) -> None:
        self._analyzer = _build_analyzer(spacy_model, custom_patterns)
        self._anonymizer = AnonymizerEngine()
        self._entities = list(replacements)
        self._operators = {
            entity: OperatorConfig("replace", {"new_value": replacement})
            for entity, replacement in replacements.items()
        }
        self._score_threshold = score_threshold
        self._chunk_chars = chunk_chars
        self._limiter = anyio.Semaphore(concurrency)
        log.info("pii.engine_ready", model=spacy_model, entities=len(self._entities))

    async def redact(self, text: str) -> RedactedText:
        if not text.strip():
            return RedactedText(text=text, entity_counts={}, redactor=REDACTOR)
        async with self._limiter:
            return await anyio.to_thread.run_sync(self._redact, text)

    def _redact(self, text: str) -> RedactedText:
        started = time.perf_counter()
        counts: dict[str, int] = {}
        pieces: list[str] = []
        for chunk in _chunks(text, self._chunk_chars):
            results = self._analyzer.analyze(
                text=chunk,
                language="en",
                entities=self._entities,
                score_threshold=self._score_threshold,
            )
            # presidio's NER labels bare alphanumeric codes (e.g. "M54.5") as
            # locations. An ICD-10 code surviving redaction is a functional
            # requirement — the codes are what the entire downstream evaluation
            # runs on — so any match whose full span IS a code (not merely
            # containing one) is dropped before counting or anonymising.
            results = [
                result
                for result in results
                if not ICD10_TOKEN_RE.fullmatch(chunk[result.start : result.end])
            ]
            for result in results:
                counts[result.entity_type] = counts.get(result.entity_type, 0) + 1
            pieces.append(
                # presidio-anonymizer's stub for `analyzer_results` names its own
                # RecognizerResult class, not presidio-analyzer's — the two are
                # structurally identical and this is the documented usage pattern
                # across presidio's own examples.
                self._anonymizer.anonymize(
                    text=chunk,
                    analyzer_results=results,  # type: ignore[arg-type]
                    operators=self._operators,
                ).text
            )
        # Never log the text or a sample of it — counts and duration only (ADR-001).
        duration_ms = (time.perf_counter() - started) * 1000
        log.info(
            "pii.redacted",
            entities=sum(counts.values()),
            characters=len(text),
            duration_ms=round(duration_ms, 1),
        )
        return RedactedText(text="\n\n".join(pieces), entity_counts=counts, redactor=REDACTOR)


def _chunks(text: str, limit: int) -> list[str]:
    """Split on paragraph breaks into pieces no larger than `limit`.

    An entity that straddles a `\\n\\n` boundary is missed; that is an accepted
    limitation of chunking, and the paragraph split makes it rare in clinical notes.
    A single paragraph longer than `limit` is passed through whole rather than cut
    mid-sentence, since a hard cut would break more entities than it saves memory.
    """
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    # `None`, not `""`, is the sentinel: an empty leading/trailing paragraph is a
    # real paragraph and must be accumulated, not treated as "nothing yet".
    current: str | None = None
    for paragraph in text.split("\n\n"):
        candidate = f"{current}\n\n{paragraph}" if current is not None else paragraph
        if current is not None and len(candidate) > limit:
            chunks.append(current)
            current = paragraph
        else:
            current = candidate
    if current is not None:
        chunks.append(current)
    return chunks
