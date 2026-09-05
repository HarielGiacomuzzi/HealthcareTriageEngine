"""structlog configuration. JSON to stdout, with an ADR-001 redaction guard."""

import logging
from typing import Any

import structlog
from structlog.typing import EventDict

SENSITIVE_FIELDS = frozenset({"text", "raw_text", "redacted_text", "webhook_secret", "api_key"})


def drop_sensitive_fields(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Remove fields that must never reach a log sink (ADR-001)."""
    for key in SENSITIVE_FIELDS & event_dict.keys():
        del event_dict[key]
    return event_dict


def configure_logging(level: str) -> None:
    numeric_level = logging.getLevelNamesMapping()[level.upper()]
    logging.basicConfig(format="%(message)s", level=numeric_level)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            drop_sensitive_fields,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
