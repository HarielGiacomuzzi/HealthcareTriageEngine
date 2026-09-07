"""Real MinIO in Docker. The generic `DockerContainer` is used rather than a
testcontainers MinIO module so the test does not depend on which extra package
provides it in the resolved version."""

from collections.abc import AsyncIterator, Iterator
from contextlib import suppress

import pytest
from botocore.exceptions import ClientError
from testcontainers.core.container import DockerContainer
from tests.adapters.containers import wait_until

from ecet.application.errors import ObjectNotFound
from ecet.infrastructure.storage.s3 import S3ObjectStorage

BUCKET = "claims"
KEY = "tenants/tenant-a/claims/note with spaces.pdf"
BODY = b"%PDF-1.7 not really a pdf, just bytes"


@pytest.fixture(scope="session")
def minio_endpoint() -> Iterator[str]:
    container = (
        DockerContainer("minio/minio:RELEASE.2025-09-07T16-13-09Z")
        .with_command("server /data")
        .with_env("MINIO_ROOT_USER", "minioadmin")
        .with_env("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_exposed_ports(9000)
    )
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(9000)
        yield f"http://{host}:{port}"


@pytest.fixture
async def storage(minio_endpoint: str) -> AsyncIterator[S3ObjectStorage]:
    adapter = S3ObjectStorage(
        endpoint_url=minio_endpoint, access_key="minioadmin", secret_key="minioadmin"
    )

    async def create_bucket() -> None:
        async with adapter.client() as client:
            # MinIO's "already exists" error code varies by release; the retry
            # loop's job is reaching a usable bucket, not classifying this one.
            with suppress(ClientError):
                await client.create_bucket(Bucket=BUCKET)

    # MinIO answers the port before it answers the API; retry until a call succeeds.
    await wait_until(create_bucket)
    yield adapter


async def test_round_trip(storage: S3ObjectStorage) -> None:
    async with storage.client() as client:
        await client.put_object(Bucket=BUCKET, Key=KEY, Body=BODY)

    assert await storage.get_bytes(BUCKET, KEY) == BODY


async def test_head_returns_the_etag_and_size(storage: S3ObjectStorage) -> None:
    async with storage.client() as client:
        await client.put_object(Bucket=BUCKET, Key=KEY, Body=BODY)

    head = await storage.head(BUCKET, KEY)

    assert head.size == len(BODY)
    assert head.etag and '"' not in head.etag


async def test_a_missing_key_raises_object_not_found(storage: S3ObjectStorage) -> None:
    with pytest.raises(ObjectNotFound):
        await storage.get_bytes(BUCKET, "tenants/tenant-a/claims/absent.pdf")


async def test_a_missing_key_raises_object_not_found_on_head(
    storage: S3ObjectStorage,
) -> None:
    with pytest.raises(ObjectNotFound):
        await storage.head(BUCKET, "tenants/tenant-a/claims/absent.pdf")


async def test_a_missing_bucket_raises_object_not_found(storage: S3ObjectStorage) -> None:
    with pytest.raises(ObjectNotFound):
        await storage.get_bytes("no-such-bucket", KEY)
