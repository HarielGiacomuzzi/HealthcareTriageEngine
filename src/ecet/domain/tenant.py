"""Tenant: owns webhook configuration and the data-isolation boundary."""

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, SecretStr

from ecet.domain.ids import TenantId, TenantIdField

__all__ = ["Tenant", "TenantId", "TenantIdField"]


class Tenant(BaseModel):
    """A client of the platform. Never mutated in v1 — seeded and read."""

    model_config = ConfigDict(frozen=True)

    id: TenantIdField
    name: str = Field(min_length=1)
    webhook_url: HttpUrl
    webhook_secret: SecretStr
    active: bool = True
