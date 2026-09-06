"""Route-edge dependencies: auth and container access.

Auth is v1-simple on purpose — a static API key and a tenant header. Production would
use per-tenant keys or JWT; the README says so. `compare_digest` is used anyway,
because a timing-safe comparison costs nothing.
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
    if x_api_key is None or not compare_digest(x_api_key, expected):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing X-API-Key")


def require_event_token(
    container: ContainerDep,
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    """MinIO sends `Authorization: Bearer <ECET_S3_EVENT_TOKEN>`."""
    expected = container.settings.s3_event_token.get_secret_value()
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not compare_digest(token, expected):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing bearer token")


def require_tenant_id(x_tenant_id: Annotated[str, Header()]) -> TenantId:
    """The tenant a read is scoped to. Repositories do not filter by tenant, so every
    route that returns tenant data compares this against the record it loaded."""
    return TenantId(x_tenant_id)


TenantDep = Annotated[TenantId, Depends(require_tenant_id)]
