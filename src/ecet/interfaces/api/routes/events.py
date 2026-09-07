"""`POST /v1/events/s3` — the MinIO bucket notification endpoint.

Two response shapes, because MinIO sends one record per notification in the compose
setup but the S3 format allows many:

- one actionable record → the plain `IngestResult` (plus `ignored`, if any records
  were skipped), or the mapped error status;
- many → 207 with one entry per record, so a single bad object does not discard the
  good ones. (Not a real WebDAV multi-status body — the api spec calls it
  "207-style".)

Anything that is not an `ObjectCreated:*` event on a `.pdf` key answers 200 and is
counted as ignored. The compose webhook target sets no `queue_dir`, so MinIO's
notification target is not persistent: a failed delivery is logged and dropped, not
retried. A 200 here is still the right answer for a non-claim event (a delete
notification has nothing to re-send), but the same non-persistence means a genuine
4xx on an actionable event loses it silently — see the module docstring in
`errors.py` and the Phase 3 carry-over list for that consequence.
"""

from typing import Any
from urllib.parse import unquote_plus

import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from ecet.application.use_cases.ingest_claim_document import IngestCommand
from ecet.interfaces.api.dependencies import ContainerDep, require_event_token

log = structlog.get_logger(__name__)

router = APIRouter(tags=["events"])

CREATED_PREFIX = "s3:ObjectCreated:"

#: MinIO's webhook has a 30 s timeout and retries any non-2xx response; a sequential,
#: unbounded batch could blow past that budget under a bulk `mc mirror`. MinIO's own
#: notifications are always one record per call, so this bound is generous, not tight.
MAX_RECORDS = 100


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
        # An absent `eventName` is treated as a creation event (fail-open, not
        # fail-closed): an authenticated MinIO payload missing the field is still a
        # real object drop in practice, and failing closed would silently stop
        # ingestion if MinIO ever omitted it.
        created = not self.event_name or self.event_name.startswith(CREATED_PREFIX)
        return created and self.decoded_key().endswith(".pdf")

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
    if len(envelope.records) > MAX_RECORDS:
        return JSONResponse(
            {"detail": f"a notification may not carry more than {MAX_RECORDS} records"},
            status_code=400,
        )

    # (original index in `envelope.records`, record) — the index a caller gets back
    # must be the position they sent, not the position after filtering.
    actionable = [(i, r) for i, r in enumerate(envelope.records) if r.is_claim_pdf()]
    ignored = len(envelope.records) - len(actionable)
    if not actionable:
        log.info("s3_event.ignored", ignored=ignored)
        return JSONResponse({"ignored": ignored}, status_code=200)

    if len(actionable) == 1:
        # Let the error handlers map a failure to its status — the single-record case
        # is the one MinIO actually sends, and it wants a real status code.
        result = await container.ingest.execute(actionable[0][1].to_command())
        body: dict[str, Any] = result.model_dump(mode="json")
        if ignored:
            body["ignored"] = ignored
        return JSONResponse(body, status_code=200)

    results: list[dict[str, Any]] = []
    for index, record in actionable:
        # Indexed by position in `Records`, not the bucket: every record in a batch
        # shares one bucket, so the bucket name identifies nothing and is
        # attacker-controlled — the one client-supplied string that would otherwise
        # reach a response body.
        entry: dict[str, Any] = {"index": index}
        try:
            entry["result"] = (await container.ingest.execute(record.to_command())).model_dump(
                mode="json"
            )
        except Exception as error:
            # Any failure, not just a `DomainError` — an unexpected exception on one
            # record must not discard results already committed for the others.
            entry["error"] = type(error).__name__
            log.warning("s3_event.record_failed", error=type(error).__name__)
        results.append(entry)
    return JSONResponse({"results": results, "ignored": ignored}, status_code=207)
