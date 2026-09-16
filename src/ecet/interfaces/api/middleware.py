"""Request context.

`request_id` is carried in `structlog.contextvars`, not in a parameter, because the
one place it has to reach besides the log line is three layers down: the queue
publisher stamps it on the message so the worker's lines carry the same id. Threading
a field only logging reads through every use case signature would be worse.
"""

from collections.abc import Awaitable, Callable
from uuid import uuid4

import structlog
from fastapi import Request, Response

REQUEST_ID_HEADER = "x-request-id"


async def bind_request_id(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Bind one id for the request and echo it back, so a caller reporting a problem
    can quote the same string an operator greps for."""
    request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid4())
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=request_id)
    try:
        response = await call_next(request)
    finally:
        # Starlette runs middleware in a task per request, but the context is copied,
        # not owned: clearing keeps a pooled task from inheriting a stale id.
        structlog.contextvars.clear_contextvars()
    response.headers[REQUEST_ID_HEADER] = request_id
    return response
