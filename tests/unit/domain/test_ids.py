import re
from uuid import uuid4

from ecet.domain.ids import TENANT_ID_PATTERN, ClaimId, PolicyId, TenantId


def test_ids_are_transparent_at_runtime() -> None:
    """NewType is a typing-only wrapper: the value is the plain UUID / str."""
    raw = uuid4()
    assert ClaimId(raw) == raw
    assert PolicyId(raw) == raw
    assert TenantId("tenant-a") == "tenant-a"


def test_tenant_id_pattern_matches_the_s3_key_segment() -> None:
    assert re.match(TENANT_ID_PATTERN, "tenant-a")
    assert re.match(TENANT_ID_PATTERN, "t1")
    assert not re.match(TENANT_ID_PATTERN, "Tenant-A")
    assert not re.match(TENANT_ID_PATTERN, "a")
