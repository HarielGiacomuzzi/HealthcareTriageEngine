"""Domain errors → RFC 7807 problem responses.

The rule that shapes this file: **`detail` is a constant, never the exception's
message.** `InvalidObjectKey` and `ObjectNotFound` both embed the client-supplied
object key, which carries a filename that may itself be PII. The message still goes
to the log, where the redaction guard and access controls apply; it does not go to
the client.
"""

from typing import NamedTuple

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic_core import ValidationError

from ecet.application.errors import ExtractionFailed, ObjectNotFound, QueuePublishError
from ecet.domain.errors import (
    ClaimNotFound,
    ConcurrentModification,
    DomainError,
    InvalidObjectKey,
    InvalidTransition,
    NoPoliciesForTenant,
    PdfTooLarge,
    ReviewAlreadyResolved,
    ReviewTaskNotFound,
    TenantNotFound,
)

log = structlog.get_logger(__name__)

PROBLEM_MEDIA_TYPE = "application/problem+json"


class Problem(NamedTuple):
    status: int
    title: str
    detail: str


class ProblemDetails(BaseModel):
    type: str
    title: str
    status: int
    detail: str
    claim_id: str | None = None


UNKNOWN = Problem(500, "Internal error", "The request could not be completed.")

#: A domain model built by hand from client input (e.g. `SourceObject` from an S3
#: event record) raises `pydantic_core.ValidationError`, not a `DomainError` — it
#: never runs through FastAPI's own request-body validation, so FastAPI's
#: `RequestValidationError` handling (422s) never sees it. Left unmapped it falls
#: through to the catch-all as a 500 that MinIO retries forever.
INVALID_OBJECT = Problem(
    400, "Invalid object", "The object does not have a valid bucket, key, etag and size."
)

PROBLEMS: dict[type[DomainError], Problem] = {
    InvalidObjectKey: Problem(
        400,
        "Invalid object key",
        "The object key must look like tenants/{tenant_id}/claims/{name}.pdf.",
    ),
    PdfTooLarge: Problem(413, "PDF too large", "The object exceeds the configured size limit."),
    TenantNotFound: Problem(404, "Tenant not found", "No active tenant for that object key."),
    ObjectNotFound: Problem(404, "Object not found", "No object at that bucket and key."),
    ClaimNotFound: Problem(404, "Claim not found", "No claim with that id for this tenant."),
    ReviewTaskNotFound: Problem(404, "Review task not found", "No review task with that id."),
    ExtractionFailed: Problem(
        422, "Text extraction failed", "No usable text could be read from the document."
    ),
    NoPoliciesForTenant: Problem(
        422, "No active policies", "The tenant has no active policy effective today."
    ),
    ReviewAlreadyResolved: Problem(
        409, "Review already resolved", "That review task has already been resolved."
    ),
    ConcurrentModification: Problem(
        409, "Concurrent modification", "The claim changed while the request was running."
    ),
    InvalidTransition: Problem(
        409, "Invalid state transition", "The claim is not in a state that allows this."
    ),
    QueuePublishError: Problem(
        503, "Queue unavailable", "The evaluation could not be queued; retry later."
    ),
}


def problem_for(error: BaseException) -> Problem:
    for candidate in type(error).__mro__:
        problem = PROBLEMS.get(candidate)  # type: ignore[arg-type]  # mro walks past DomainError
        if problem is not None:
            return problem
    return UNKNOWN


def register_error_handlers(app: FastAPI) -> None:
    async def handle(request: Request, error: Exception) -> JSONResponse:
        problem = problem_for(error)
        claim_id = getattr(error, "claim_id", None)
        # The message is logged, not returned: it may embed a client-supplied filename.
        log.warning(
            "api.domain_error",
            error=type(error).__name__,
            message=str(error),
            status=problem.status,
            claim_id=claim_id,
            path=request.url.path,
        )
        body = ProblemDetails(
            type=f"https://ecet.invalid/problems/{type(error).__name__}",
            title=problem.title,
            status=problem.status,
            detail=problem.detail,
            claim_id=str(claim_id) if claim_id is not None else None,
        )
        return JSONResponse(
            body.model_dump(exclude_none=True),
            status_code=problem.status,
            media_type=PROBLEM_MEDIA_TYPE,
        )

    async def handle_unmapped(request: Request, error: Exception) -> JSONResponse:
        # A generic-`Exception` handler still sits behind FastAPI's own HTTPException
        # handler in Starlette's mro-based lookup, so a 401/404 raised via
        # `HTTPException` is untouched by this — only a truly unhandled exception
        # (a bug) reaches here.
        log.error("api.unhandled_error", error=type(error).__name__, path=request.url.path)
        return JSONResponse(
            ProblemDetails(
                type="https://ecet.invalid/problems/Unknown",
                title=UNKNOWN.title,
                status=UNKNOWN.status,
                detail=UNKNOWN.detail,
            ).model_dump(exclude_none=True),
            status_code=UNKNOWN.status,
            media_type=PROBLEM_MEDIA_TYPE,
        )

    async def handle_invalid_object(request: Request, error: Exception) -> JSONResponse:
        log.warning(
            "api.invalid_object",
            error=type(error).__name__,
            message=str(error),
            path=request.url.path,
        )
        return JSONResponse(
            ProblemDetails(
                type="https://ecet.invalid/problems/InvalidObject",
                title=INVALID_OBJECT.title,
                status=INVALID_OBJECT.status,
                detail=INVALID_OBJECT.detail,
            ).model_dump(exclude_none=True),
            status_code=INVALID_OBJECT.status,
            media_type=PROBLEM_MEDIA_TYPE,
        )

    app.add_exception_handler(DomainError, handle)
    app.add_exception_handler(ValidationError, handle_invalid_object)
    app.add_exception_handler(Exception, handle_unmapped)
