"""Policy: a tenant-owned rule saying when a diagnosis justifies a service.

Read-only in v1 — policies are seeded, not managed through the API.
"""

from datetime import date
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ecet.domain.ids import PolicyId, TenantIdField

__all__ = ["ICD10_PATTERN", "Icd10Code", "Policy", "PolicyId"]

ICD10_PATTERN = r"^[A-TV-Z][0-9][0-9A-Z](\.[0-9A-Z]{1,4})?$"


class Icd10Code(BaseModel):
    """A single normalised ICD-10 code. Frozen, therefore hashable and set-safe."""

    model_config = ConfigDict(frozen=True)

    code: Annotated[str, Field(pattern=ICD10_PATTERN)]

    @field_validator("code", mode="before")
    @classmethod
    def _normalise(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value

    def __str__(self) -> str:
        return self.code


class Policy(BaseModel):
    """One version of one coverage rule. Only the highest active version is used."""

    model_config = ConfigDict(frozen=True)

    id: PolicyId
    tenant_id: TenantIdField
    name: str = Field(min_length=1)
    version: int = Field(ge=1)
    covered_codes: set[Icd10Code] = Field(default_factory=set)
    excluded_codes: set[Icd10Code] = Field(default_factory=set)
    criteria_text: str = Field(min_length=1)
    required_evidence: list[str] = Field(default_factory=list)
    active: bool = True
    effective_from: date
    effective_to: date | None = None

    @model_validator(mode="after")
    def _check_invariants(self) -> Self:
        overlap = self.covered_codes & self.excluded_codes
        if overlap:
            listed = ", ".join(sorted(item.code for item in overlap))
            raise ValueError(f"codes are both covered and excluded: {listed}")
        if self.effective_to is not None and self.effective_to < self.effective_from:
            raise ValueError("effective_to must not precede effective_from")
        return self

    def is_effective(self, on: date) -> bool:
        """Active and inside the effective window (both ends inclusive)."""
        return (
            self.active
            and self.effective_from <= on
            and (self.effective_to is None or on <= self.effective_to)
        )
