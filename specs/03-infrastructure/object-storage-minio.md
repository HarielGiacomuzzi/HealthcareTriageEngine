# Infra: Object Storage (MinIO / S3)

Module: `ecet/infrastructure/storage/s3.py`. Implements `ObjectStorage` port.

## Adapter
- `aiobotocore` (or `boto3` in thread via `anyio.to_thread`) with endpoint override
  `ECET_S3_ENDPOINT` (MinIO in compose, real AWS in prod: same code).
- `get_bytes(bucket, key)` → `bytes`. Missing → `ObjectNotFound`. Streams to memory; size already bounded by `SourceObject`.

## Event Delivery
MinIO bucket notification → webhook target → [`POST /v1/events/s3`](../04-interfaces/api.md#s3-event-handling).

Compose init container ([`minio-setup`](../05-platform/docker-compose.md#docker-composeyml-services), `mc` CLI):
```
mc alias set local http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
mc mb -p local/claims
mc admin config set local notify_webhook:ecet endpoint=http://api:8000/v1/events/s3 auth_token=$ECET_S3_EVENT_TOKEN
mc admin service restart local
mc event add local/claims arn:minio:sqs::ecet:webhook --event put --suffix .pdf
```

Event payload = S3 notification format (`Records[].s3.bucket.name`, `object.key` (URL-encoded), `object.eTag`, `object.size`).
API parses with Pydantic model `S3EventEnvelope`; unescapes key.

Auth: MinIO sends `Authorization: Bearer <auth_token>`. API checks against `ECET_S3_EVENT_TOKEN`. Prod: swap for SNS/EventBridge signature check (out of scope).

## Tests
- Adapter integration test with testcontainers MinIO: put object, `get_bytes` equals.
- Envelope parsing fixture from real MinIO event (URL-encoded key with spaces).
