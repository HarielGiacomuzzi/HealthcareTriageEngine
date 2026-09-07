"""`ObjectStorage` over the S3 API.

One code path for MinIO and AWS: the only difference is `endpoint_url`. A client is
created per call — `aiobotocore` clients are async context managers bound to the
running loop, and the cost is a local object, not a connection handshake, because
botocore pools the underlying HTTP session.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import structlog
from aiobotocore.session import get_session
from botocore.exceptions import ClientError

from ecet.application.errors import ObjectNotFound
from ecet.application.ports.object_storage import ObjectHead

log = structlog.get_logger(__name__)

#: What S3 and MinIO return for "the object, or its bucket, is not there".
_MISSING_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "404", "NotFound"})


class S3ObjectStorage:
    def __init__(
        self,
        *,
        endpoint_url: str | None,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
    ) -> None:
        self._session = get_session()
        self._client_kwargs: dict[str, Any] = {
            "endpoint_url": endpoint_url,
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": region,
        }

    @asynccontextmanager
    async def client(self) -> AsyncIterator[Any]:
        """Exposed so adapter tests can seed objects through the same credentials."""
        async with self._session.create_client("s3", **self._client_kwargs) as client:
            yield client

    async def get_bytes(self, bucket: str, key: str) -> bytes:
        async with self.client() as client:
            try:
                response = await client.get_object(Bucket=bucket, Key=key)
            except ClientError as error:
                raise _translate(error, bucket, key) from error
            async with response["Body"] as stream:
                data: bytes = await stream.read()
        # Never log the key at info level with the body size only — the key carries a
        # client-supplied filename. Debug-level, bucket only.
        log.debug("storage.read", bucket=bucket, bytes=len(data))
        return data

    async def head(self, bucket: str, key: str) -> ObjectHead:
        async with self.client() as client:
            try:
                response = await client.head_object(Bucket=bucket, Key=key)
            except ClientError as error:
                raise _translate(error, bucket, key) from error
        # S3 quotes etags; `SourceObject` compares them as plain strings.
        return ObjectHead(
            etag=str(response["ETag"]).strip('"'), size=int(response["ContentLength"])
        )


def _translate(error: ClientError, bucket: str, key: str) -> Exception:
    code = str(error.response.get("Error", {}).get("Code", ""))
    if code in _MISSING_CODES:
        return ObjectNotFound(f"{bucket}/{key}")
    # `ClientError` carries no stubs, so it types as `Any`; without the cast mypy
    # flags this as returning `Any` from a function declared to return `Exception`.
    return cast(Exception, error)
