"""Object storage contract. MinIO in compose, S3 in production — same interface."""

from typing import NamedTuple, Protocol, runtime_checkable


class ObjectHead(NamedTuple):
    """What a HEAD tells us: the two fields `SourceObject` needs."""

    etag: str
    size: int


@runtime_checkable
class ObjectStorage(Protocol):
    async def get_bytes(self, bucket: str, key: str) -> bytes:
        """Raises `ObjectNotFound`. Reads into memory; `SourceObject` already bounded
        the size."""
        ...

    async def head(self, bucket: str, key: str) -> ObjectHead:
        """Raises `ObjectNotFound`. Used by `POST /v1/claims/ingest`, which receives
        only `{bucket, key}` and has to fill in the idempotency key itself."""
        ...
