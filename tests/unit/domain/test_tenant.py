from typing import Any

import pytest
from pydantic import SecretStr, ValidationError

from ecet.domain.tenant import Tenant


def build_tenant(**overrides: Any) -> Tenant:
    fields: dict[str, Any] = {
        "id": "tenant-a",
        "name": "Tenant A",
        "webhook_url": "http://mock-client:9000/hooks/ecet",
        "webhook_secret": SecretStr("s3cret"),
    }
    fields.update(overrides)
    return Tenant.model_validate(fields)


@pytest.mark.parametrize("raw", ["tenant-a", "t1", "a0-b1-c2", "x" * 63])
def test_valid_tenant_ids_are_accepted(raw: str) -> None:
    assert build_tenant(id=raw).id == raw


@pytest.mark.parametrize(
    "raw",
    ["", "a", "-lead", "UPPER", "under_score", "tenant a", "x" * 64],
)
def test_invalid_tenant_ids_are_rejected(raw: str) -> None:
    with pytest.raises(ValidationError):
        build_tenant(id=raw)


def test_tenant_is_frozen() -> None:
    tenant = build_tenant()
    with pytest.raises(ValidationError):
        tenant.name = "renamed"  # type: ignore[misc]


def test_tenant_defaults_to_active() -> None:
    assert build_tenant().active is True


def test_webhook_secret_never_appears_in_a_dump() -> None:
    """ADR-001 hygiene: the HMAC key must not leak through model_dump / repr."""
    tenant = build_tenant()
    assert "s3cret" not in str(tenant.model_dump())
    assert "s3cret" not in repr(tenant)
    assert tenant.webhook_secret.get_secret_value() == "s3cret"


def test_empty_name_is_rejected() -> None:
    with pytest.raises(ValidationError):
        build_tenant(name="")
