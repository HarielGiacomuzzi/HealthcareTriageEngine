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

The publish (step 10) happens *after* the `POLICIES_ATTACHED` commit. A publish that
fails therefore leaves a durable `POLICIES_ATTACHED` claim that a retry can re-send;
the transactional outbox that would make this atomic is deferred out of v1.
"""

from collections.abc import Callable, Sequence
from uuid import uuid4

import structlog
from pydantic import BaseModel, ConfigDict

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
        # `SourceObject` validates the key layout and yields the tenant; both failures
        # happen before any I/O, so a malformed event costs one validation.
        source = SourceObject(
            bucket=command.bucket, key=command.key, etag=command.etag, size=command.size
        )
        source.ensure_size_within(self._max_pdf_bytes)
        tenant_id = source.tenant_id()

        async with self._uow_factory() as uow:
            duplicate = await uow.claims.find_by_source(source.bucket, source.key, source.etag)
            if duplicate is not None:
                # ADR-006: same object, same content — nothing to redo.
                log.info(
                    "claim.duplicate",
                    claim_id=str(duplicate.id),
                    tenant_id=str(tenant_id),
                    status=duplicate.status.value,
                )
                return IngestResult(claim_id=duplicate.id, status=duplicate.status, duplicate=True)

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

            await self._run_pipeline(uow, claim)
            await uow.commit()
            return IngestResult(claim_id=claim.id, status=claim.status)

    async def _run_pipeline(self, uow: UnitOfWork, claim: Claim) -> None:
        # Both depend on repositories owned by this unit of work, so they are built
        # here rather than injected — a request-scoped dependency cannot be a
        # process-scoped constructor argument.
        attach_policies = AttachTenantPolicies(uow.policies, self._clock)
        request_review = RequestHumanReview(uow.review_tasks, self._clock)

        try:
            text = await self._read_text(claim.source)
        except ExtractionFailed as error:
            await self._fail(uow, claim, ClaimStatus.EXTRACTION_FAILED, str(error))
            raise
        self._advance(claim, ClaimStatus.EXTRACTED)

        redacted = await self._redact_pii.execute(text)
        del text  # ADR-001: the raw note stops existing here.
        claim.redacted = redacted
        self._advance(claim, ClaimStatus.REDACTED)

        try:
            policies: Sequence[Policy] = await attach_policies.execute(claim.tenant_id)
        except NoPoliciesForTenant as error:
            await self._fail(uow, claim, ClaimStatus.NO_POLICIES, str(error))
            raise
        claim.policy_ids = [policy.id for policy in policies]
        self._advance(claim, ClaimStatus.POLICIES_ATTACHED)

        deterministic = await self._run_checks.execute(redacted, policies)
        claim.deterministic = deterministic

        if deterministic.verdict is Verdict.REJECT:
            # ADR-002's saving: a human looks at it, the LLM is never called.
            await request_review.execute(claim, ReviewReason.DETERMINISTIC_REJECT)
            self._advance(claim, ClaimStatus.REVIEW_PENDING)
            await uow.claims.save(claim)
            return

        # Commit before publishing: a publish failure must leave a durable
        # POLICIES_ATTACHED claim, not a rolled-back one.
        await uow.claims.save(claim)
        await uow.commit()

        await self._enqueue.execute(claim, policies)
        self._advance(claim, ClaimStatus.QUEUED)
        await uow.claims.save(claim)

    async def _read_text(self, source: SourceObject) -> str:
        try:
            data = await self._storage.get_bytes(source.bucket, source.key)
        except ObjectNotFound as error:
            # The message embeds the client-supplied key; the reason token must not.
            raise ExtractionFailed("object_unavailable") from error
        text = await self._extractor.extract(data)
        if not text.strip():
            raise ExtractionFailed("empty_text")
        return text

    def _advance(self, claim: Claim, status: ClaimStatus) -> None:
        claim.transition(status, now=self._clock.now())
        log.info(
            "claim.transition",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=status.value,
        )

    async def _fail(self, uow: UnitOfWork, claim: Claim, status: ClaimStatus, reason: str) -> None:
        claim.transition(status, reason=reason, now=self._clock.now())
        log.warning(
            "claim.failed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=status.value,
            reason=reason,
        )
        await uow.claims.save(claim)
        await uow.commit()
