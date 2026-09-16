"""UC-01 IngestClaimDocument — the ingestion orchestrator.

Two properties the code is arranged around:

**ADR-001.** The raw extracted text exists as exactly one local variable, `text`, in
`_run_pipeline`. It is passed to `RedactPii` and then deleted. It is never assigned to
the claim, never logged, never returned.

**Auditability.** The claim row is inserted and committed before any of the fallible
steps run, so a failure leaves a claim to look at rather than nothing at all. The two
recoverable failure states the state machine allows from that point —
`EXTRACTION_FAILED` and `NO_POLICIES` — are persisted and committed on their own path
before the error is re-raised.

**No connection across slow work.** The object read, pypdf, spaCy and the publish each
run with no unit of work open: read and insert, close; fetch, extract and redact;
re-read, attach policies, check and save, close; publish; re-read and mark `QUEUED`.
Each write re-reads the claim, because `ClaimRepository.save` refuses a claim this unit
of work did not load.

The publish happens *after* the `POLICIES_ATTACHED` commit. A publish that fails
therefore leaves a durable `POLICIES_ATTACHED` claim that a retry can re-send; the
transactional outbox that would make this atomic is deferred out of v1. The retry is
the same object arriving again — MinIO re-sending the event it got a 503 for, or an
operator re-posting `/v1/claims/ingest`: the duplicate check re-publishes a claim it
finds still `POLICIES_ATTACHED` instead of returning it untouched. A worker that reads
the claim before the `QUEUED` write lands requeues the message (UC-06,
`ClaimNotYetQueued`).
"""

import time
from collections.abc import Callable, Sequence
from uuid import uuid4

import structlog
from pydantic import BaseModel, ConfigDict

from ecet import metrics
from ecet.application.errors import ExtractionFailed, ObjectNotFound
from ecet.application.ports.clock import Clock
from ecet.application.ports.object_storage import ObjectStorage
from ecet.application.ports.text_extractor import TextExtractor
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.use_cases.attach_tenant_policies import AttachTenantPolicies
from ecet.application.use_cases.enqueue_evaluation import EnqueueEvaluation
from ecet.application.use_cases.human_review import RequestHumanReview
from ecet.application.use_cases.redact_pii import RedactPii
from ecet.application.use_cases.run_deterministic_checks import RunDeterministicChecks
from ecet.domain.claim import Claim, ClaimStatus, SourceObject
from ecet.domain.errors import NoPoliciesForTenant
from ecet.domain.evaluation import ReviewReason, Verdict
from ecet.domain.ids import ClaimId
from ecet.domain.policy import Policy

log = structlog.get_logger(__name__)


class IngestCommand(BaseModel):
    model_config = ConfigDict(frozen=True)

    bucket: str
    key: str
    etag: str
    size: int


class IngestResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    claim_id: ClaimId
    status: ClaimStatus
    duplicate: bool = False


class IngestClaimDocument:
    def __init__(
        self,
        *,
        uow_factory: Callable[[], UnitOfWork],
        storage: ObjectStorage,
        extractor: TextExtractor,
        redact_pii: RedactPii,
        run_checks: RunDeterministicChecks,
        enqueue: EnqueueEvaluation,
        clock: Clock,
        max_pdf_bytes: int,
    ) -> None:
        self._uow_factory = uow_factory
        self._storage = storage
        self._extractor = extractor
        self._redact_pii = redact_pii
        self._run_checks = run_checks
        self._enqueue = enqueue
        self._clock = clock
        self._max_pdf_bytes = max_pdf_bytes

    async def execute(self, command: IngestCommand) -> IngestResult:
        started = time.perf_counter()
        try:
            result = await self._execute(command)
        except BaseException:
            metrics.INGEST_SECONDS.labels(outcome="failed").observe(time.perf_counter() - started)
            raise
        outcome = "duplicate" if result.duplicate else "ingested"
        metrics.INGEST_SECONDS.labels(outcome=outcome).observe(time.perf_counter() - started)
        return result

    async def _execute(self, command: IngestCommand) -> IngestResult:
        # `SourceObject` validates the key layout and yields the tenant; both failures
        # happen before any I/O, so a malformed event costs one validation.
        source = SourceObject(
            bucket=command.bucket, key=command.key, etag=command.etag, size=command.size
        )
        source.ensure_size_within(self._max_pdf_bytes)
        tenant_id = source.tenant_id()

        republish: Sequence[Policy] | None = None
        async with self._uow_factory() as uow:
            duplicate = await uow.claims.find_by_source(source.bucket, source.key, source.etag)
            if duplicate is not None:
                if duplicate.status is ClaimStatus.POLICIES_ATTACHED:
                    # Against the policy versions attached the first time, not whatever
                    # is active today.
                    republish = await uow.policies.get_many(duplicate.policy_ids)
            else:
                await uow.tenants.get(tenant_id)  # raises TenantNotFound; no claim is created
                now = self._clock.now()
                claim = Claim(
                    id=ClaimId(uuid4()),
                    tenant_id=tenant_id,
                    source=source,
                    created_at=now,
                    updated_at=now,
                )
                await uow.claims.add(claim)
                await uow.commit()

        if duplicate is None:
            return await self._run_pipeline(claim)

        status = duplicate.status
        if republish is not None:
            # The retry for a failed publish: the claim already holds the redacted
            # text, the deterministic result and its policy ids, so only the publish
            # repeats. Raises `QueuePublishError` again if the broker is still
            # refusing, leaving the claim exactly as it was.
            await self._enqueue.execute(duplicate, republish)
            status = await self._mark_queued(duplicate.id)
        # ADR-006: same object, same content — nothing else to redo.
        log.info(
            "claim.duplicate",
            claim_id=str(duplicate.id),
            tenant_id=str(tenant_id),
            status=status.value,
        )
        return IngestResult(claim_id=duplicate.id, status=status, duplicate=True)

    async def _run_pipeline(self, received: Claim) -> IngestResult:
        try:
            text = await self._read_text(received.source)
        except ExtractionFailed as error:
            async with self._uow_factory() as uow:
                claim = await uow.claims.get(received.id)
                await self._fail(uow, claim, ClaimStatus.EXTRACTION_FAILED, str(error))
            raise

        redacted = await self._redact_pii.execute(text)
        del text  # ADR-001: the raw note stops existing here.

        async with self._uow_factory() as uow:
            claim = await uow.claims.get(received.id)
            self._advance(claim, ClaimStatus.EXTRACTED)
            claim.redacted = redacted
            self._advance(claim, ClaimStatus.REDACTED)

            # Both depend on repositories owned by this unit of work, so they are built
            # here rather than injected.
            attach_policies = AttachTenantPolicies(uow.policies, self._clock)
            try:
                policies: Sequence[Policy] = await attach_policies.execute(claim.tenant_id)
            except NoPoliciesForTenant:
                await self._fail(uow, claim, ClaimStatus.NO_POLICIES, "no_policies")
                raise
            claim.policy_ids = [policy.id for policy in policies]
            self._advance(claim, ClaimStatus.POLICIES_ATTACHED)

            deterministic = await self._run_checks.execute(redacted, policies)
            claim.deterministic = deterministic
            if deterministic.verdict is Verdict.REJECT:
                # ADR-002's saving: a human looks at it, the LLM is never called.
                request_review = RequestHumanReview(uow.review_tasks, self._clock)
                await request_review.execute(claim, ReviewReason.DETERMINISTIC_REJECT)
                self._advance(claim, ClaimStatus.REVIEW_PENDING)

            # Commit before publishing: a publish failure must leave a durable
            # POLICIES_ATTACHED claim, not a rolled-back one.
            await uow.claims.save(claim)
            await uow.commit()

        if claim.status is ClaimStatus.REVIEW_PENDING:
            return IngestResult(claim_id=claim.id, status=claim.status)

        await self._enqueue.execute(claim, policies)
        return IngestResult(claim_id=claim.id, status=await self._mark_queued(claim.id))

    async def _mark_queued(self, claim_id: ClaimId) -> ClaimStatus:
        """The write after a publish, in its own unit of work. A claim no longer
        `POLICIES_ATTACHED` was marked `QUEUED` by a concurrent re-publish of the same
        object; it is left as it is."""
        async with self._uow_factory() as uow:
            claim = await uow.claims.get(claim_id)
            if claim.status is ClaimStatus.POLICIES_ATTACHED:
                self._advance(claim, ClaimStatus.QUEUED)
                await uow.claims.save(claim)
                await uow.commit()
            return claim.status

    async def _read_text(self, source: SourceObject) -> str:
        try:
            data = await self._storage.get_bytes(source.bucket, source.key)
        except ObjectNotFound as error:
            # The message embeds the client-supplied key; the reason token must not.
            raise ExtractionFailed("object_unavailable") from error
        text = await self._extractor.extract(data)
        if not text.strip():
            raise ExtractionFailed("no_text")
        return text

    def _advance(self, claim: Claim, status: ClaimStatus) -> None:
        previous = claim.status
        claim.transition(status, now=self._clock.now())
        log.info(
            "claim.transition",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=status.value,
            **{"from": previous.value},
        )

    async def _fail(self, uow: UnitOfWork, claim: Claim, status: ClaimStatus, reason: str) -> None:
        previous = claim.status
        claim.transition(status, reason=reason, now=self._clock.now())
        log.warning(
            "claim.failed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=status.value,
            reason=reason,
            **{"from": previous.value},
        )
        await uow.claims.save(claim)
        await uow.commit()
