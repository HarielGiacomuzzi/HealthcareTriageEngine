"""ICD-10 catalogue read contract.

Seeded reference data, not tenant data. Phase 4 uses it to reject pattern matches
that are not real codes (`extract_icd10_codes` fires on "B12" in "Vitamin B12").
"""

from typing import Protocol, runtime_checkable

from ecet.domain.policy import Icd10Code


@runtime_checkable
class Icd10CodeRepository(Protocol):
    async def known_codes(self) -> frozenset[Icd10Code]:
        """Every code in the catalogue. Small enough (hundreds of rows) to read whole."""
        ...
