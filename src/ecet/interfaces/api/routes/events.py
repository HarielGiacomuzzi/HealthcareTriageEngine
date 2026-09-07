"""`POST /v1/events/s3` — the MinIO bucket notification endpoint.

Two response shapes, because MinIO sends one record per notification in the compose
setup but the S3 format allows many:

- one actionable record → the plain `IngestResult`, or the mapped error status;
- many → 207 with one entry per record, so a single bad object does not discard the
  good ones. (Not a real WebDAV multi-status body — the api spec calls it
  "207-style".)

Anything that is not an `ObjectCreated:*` event on a `.pdf` key answers 200 and is
counted as ignored: MinIO retries non-2xx, and retrying a delete notification forever
helps nobody.
"""

from typing import Any
from urllib.parse import unquote_plus

import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from ecet.application.use_cases.ingest_claim_document import IngestCommand
from ecet.domain.errors import DomainError
from ecet.interfaces.api.dependencies import ContainerDep, require_event_token

log = structlog.get_logger(__name__)

router = APIRouter(tags=["events"])

CREATED_PREFIX = "s3:ObjectCreated:"


class S3Bucket(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str


class S3Object(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    key: str
    size: int = 0
    etag: str = Field(default="", alias="eTag")


class S3Payload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    bucket: S3Bucket
    object: S3Object


class S3Record(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    event_name: str = Field(default="", alias="eventName")
    s3: S3Payload

    def is_claim_pdf(self) -> bool:
        created = not self.event_name or self.event_name.startswith(CREATED_PREFIX)
        return created and self.decoded_key().lower().endswith(".pdf")

    def decoded_key(self) -> str:
        """MinIO URL-encodes the key; spaces arrive as `+`."""
        return unquote_plus(self.s3.object.key)

    def to_command(self) -> IngestCommand:
        return IngestCommand(
            bucket=self.s3.bucket.name,
            key=self.decoded_key(),
            etag=self.s3.object.etag,
            size=self.s3.object.size,
        )


class S3EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    records: list[S3Record] = Field(default_factory=list, alias="Records")


@router.post("/v1/events/s3", dependencies=[Depends(require_event_token)])
async def receive_s3_event(envelope: S3EventEnvelope, container: ContainerDep) -> JSONResponse:
    actionable = [record for record in envelope.records if record.is_claim_pdf()]
    ignored = len(envelope.records) - len(actionable)
    if not actionable:
        log.info("s3_event.ignored", records=ignored)
        return JSONResponse({"ignored": ignored}, status_code=200)

    if len(actionable) == 1:
        # Let the error handlers map a failure to its status — the single-record case
        # is the one MinIO actually sends, and it wants a real status code.
        result = await container.ingest.execute(actionable[0].to_command())
        return JSONResponse(result.model_dump(mode="json"), status_code=200)

    results: list[dict[str, Any]] = []
    for record in actionable:
        entry: dict[str, Any] = {"bucket": record.s3.bucket.name}
        try:
            entry["result"] = (await container.ingest.execute(record.to_command())).model_dump(
                mode="json"
            )
        except DomainError as error:
            # The class name only: the message embeds the client-supplied key.
            entry["error"] = type(error).__name__
            log.warning("s3_event.record_failed", error=type(error).__name__)
        results.append(entry)
    return JSONResponse({"results": results, "ignored": ignored}, status_code=207)
