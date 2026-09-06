"""Route-edge dependencies: auth and container access.

Auth is v1-simple on purpose — a static API key and a tenant header, not per-tenant
keys or JWT. `compare_digest` is used anyway, because a timing-safe comparison costs
nothing. Headers are compared as bytes, not `str`: Starlette decodes header values as
latin-1, so a raw non-ASCII byte survives into the `str`, and `compare_digest` raises
`TypeError` on a non-ASCII `str` — `.encode("latin-1")` is the faithful inverse and
turns that crash back into an ordinary 401.
"""

from hmac import compare_digest
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status

from ecet.domain.ids import TenantId
from ecet.interfaces.api.container import ApiContainer


def get_container(request: Request) -> ApiContainer:
    container: ApiContainer = request.app.state.container
    return container


ContainerDep = Annotated[ApiContainer, Depends(get_container)]


def require_api_key(
    container: ContainerDep,
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    expected = container.settings.api_key.get_secret_value()
    if x_api_key is None or not compare_digest(
        x_api_key.encode("latin-1"), expected.encode("latin-1")
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing X-API-Key")


def require_event_token(
    container: ContainerDep,
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    """MinIO sends `Authorization: Bearer <ECET_S3_EVENT_TOKEN>`."""
    expected = container.settings.s3_event_token.get_secret_value()
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not compare_digest(
        token.encode("latin-1"), expected.encode("latin-1")
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing bearer token")


def require_tenant_id(x_tenant_id: Annotated[str, Header()]) -> TenantId:
    """The tenant a read is scoped to. Repositories do not filter by tenant, so every
    route that returns tenant data compares this against the record it loaded."""
    return TenantId(x_tenant_id)


TenantDep = Annotated[TenantId, Depends(require_tenant_id)]
