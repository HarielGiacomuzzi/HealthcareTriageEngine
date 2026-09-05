"""Identifier value objects.

Their own module so `claim`, `policy`, `evaluation` and `tenant` can each reference
another's identifier without an import cycle.
"""

from typing import Annotated, NewType
from uuid import UUID

from pydantic import StringConstraints

TENANT_ID_BODY = r"[a-z0-9][a-z0-9-]{1,62}"
TENANT_ID_PATTERN = rf"^{TENANT_ID_BODY}$"

ClaimId = NewType("ClaimId", UUID)
PolicyId = NewType("PolicyId", UUID)
TenantId = NewType("TenantId", str)

#: Use on a pydantic model field; validates the S3-key-safe tenant slug.
TenantIdField = Annotated[TenantId, StringConstraints(pattern=TENANT_ID_PATTERN)]
