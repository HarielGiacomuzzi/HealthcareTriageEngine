"""structlog configuration. JSON to stdout, with an ADR-001 redaction guard."""

import logging
import sys
from typing import Any

import structlog
from structlog.typing import EventDict, Processor

SENSITIVE_FIELDS = frozenset(
    {"text", "raw_text", "redacted_text", "notes", "webhook_secret", "api_key"}
)


def drop_sensitive_fields(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Remove fields that must never reach a log sink (ADR-001)."""
    for key in SENSITIVE_FIELDS & event_dict.keys():
        del event_dict[key]
    return event_dict


def configure_logging(level: str) -> None:
    """Configure structlog *and* the stdlib root logger, so a third-party record
    (uvicorn's, above all) is rendered by the same chain — and dropped by the same
    ADR-001 guard — as one of ours. `cli.py api` passes `log_config=None` to uvicorn
    precisely so that uvicorn does not install a handler of its own here."""
    numeric_level = logging.getLevelNamesMapping()[level.upper()]
    shared: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    structlog.configure(
        # `drop_sensitive_fields` runs last on our own records: every other processor
        # has had its say, so nothing can reintroduce a dropped key afterwards.
        processors=[
            *shared,
            drop_sensitive_fields,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        # `ExtraAdder` first: `logger.info(..., extra={"text": ...})` has to become an
        # event-dict key before `drop_sensitive_fields` can drop it.
        foreign_pre_chain=[structlog.stdlib.ExtraAdder(), *shared, drop_sensitive_fields],
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            drop_sensitive_fields,
            structlog.processors.JSONRenderer(),
        ],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(numeric_level)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        # Own handlers would render around ours; propagation is what we want instead.
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers = []
        uvicorn_logger.propagate = True
