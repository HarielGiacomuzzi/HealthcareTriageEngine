"""UC-06. The worker's whole contract lives in this class: what it does with a good
answer, what it does with a bad one, and — the part at-least-once delivery makes
non-negotiable — what it does with a message it has already handled."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from tests.fakes import (
    FakeLLMGateway,
    FakeUnitOfWork,
    FakeWebhookClient,
    FixedClock,
)
from tests.pii import assert_no_pii

from ecet.application.errors import (
    ClaimNotYetQueued,
    LLMInvalidOutput,
    LLMPermanentError,
    LLMTransientError,
    WebhookPermanentError,
)
from ecet.application.messages import EvaluationMessage, PolicySnapshot
from ecet.application.use_cases.evaluate_claim import NOT_YET_QUEUED_DELAY_S, EvaluateClaim
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.evaluation import (
    CheckOutcome,
    Decision,
    DeterministicResult,
    Evaluation,
    ReviewReason,
    Verdict,
)
from ecet.domain.ids import ClaimId, PolicyId, TenantId
from ecet.domain.policy import Icd10Code
from ecet.domain.tenant import Tenant

NOW = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)
KEY = "tenants/tenant-a/claims/note-1.pdf"
POLICY_ID = PolicyId(uuid4())
CATALOGUE = [Icd10Code(code="M54.5"), Icd10Code(code="T12")]


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": "dev-hmac-tenant-a",
        }
    )


def build_claim(*, status: ClaimStatus = ClaimStatus.QUEUED) -> Claim:
    return Claim(
        id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source=SourceObject(bucket="claims", key=KEY, etag="etag-1", size=2048),
        status=status,
        redacted=RedactedText(text="Patient <PERSON> with M54.5.", redactor="fake"),
        policy_ids=[POLICY_ID],
        deterministic=DeterministicResult(
            verdict=Verdict.PASS,
            checks=[CheckOutcome(name="icd10_present", passed=True, detail="1 code")],
        ),
        created_at=NOW,
        updated_at=NOW,
    )


def build_message(claim: Claim, *, found_codes: list[str] | None = None) -> EvaluationMessage:
    return EvaluationMessage(
        message_id=uuid4(),
        claim_id=claim.id,
        tenant_id=TenantId("tenant-a"),
        redacted_text="Patient <PERSON> with M54.5.",
        entity_counts={"PERSON": 1},
        policies=[
            PolicySnapshot(
                id=POLICY_ID,
                name="MRI lumbar spine",
                version=2,
                covered_codes=["M54.5"],
                excluded_codes=["Z00.00"],
                criteria_text="Covered after six weeks of conservative therapy.",
                required_evidence=["conservative therapy >= 6 weeks"],
            )
        ],
        deterministic_verdict="PASS",
        found_codes=["M54.5"] if found_codes is None else found_codes,
        enqueued_at=NOW,
    )


def build_case(
    uow: FakeUnitOfWork, llm: FakeLLMGateway, webhook: FakeWebhookClient
) -> EvaluateClaim:
    return EvaluateClaim(
        uow_factory=lambda: uow,
        llm=llm,
        webhook=webhook,
        clock=FixedClock(NOW),
        threshold=0.85,
        prompt_version="v1",
    )


async def build_world(
    *,
    claim: Claim | None = None,
    llm: FakeLLMGateway | None = None,
    webhook: FakeWebhookClient | None = None,
) -> tuple[EvaluateClaim, FakeUnitOfWork, Claim, FakeLLMGateway, FakeWebhookClient]:
    claim = claim if claim is not None else build_claim()
    llm = llm if llm is not None else FakeLLMGateway()
    webhook = webhook if webhook is not None else FakeWebhookClient()
    uow = FakeUnitOfWork(tenants=[build_tenant()], known_codes=CATALOGUE)
    await uow.claims.add(claim)
    return build_case(uow, llm, webhook), uow, claim, llm, webhook


async def test_a_confident_evaluation_reaches_approved_auto() -> None:
    llm = FakeLLMGateway(
        evaluation=Evaluation(
            decision=Decision.MEETS_NECESSITY,
            confidence=0.91,
            matched_policy_id=POLICY_ID,
            cited_codes=[Icd10Code(code="M54.5")],
            rationale="Conservative therapy documented for six weeks.",
            model="fake",
            prompt_version="v1",
        )
    )
    case, uow, claim, llm, webhook = await build_world(llm=llm)

    await case.execute(build_message(claim))

    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.APPROVED_AUTO
    assert stored.evaluation is not None
    assert stored.evaluation.confidence == 0.91
    assert len(llm.requests) == 1
    assert len(webhook.deliveries) == 1
    # The claim handed to `NotifyClient` must carry the evaluation the vendor just
    # returned, not the stale, unevaluated uow #1 copy — a regression here ships an
    # auto-approval to the tenant with a blank rationale and no policy reference.
    payload = webhook.deliveries[0][1]
    assert payload.rationale == "Conservative therapy documented for six weeks."
    assert payload.matched_policy_id == POLICY_ID
    assert payload.cited_codes == ["M54.5"]
    # Deviation 1: UC-06 owns the unit of work and commits *once*, so the evaluation,
    # the routing and the EVALUATED transition land in one transaction.
    assert uow.commits == 1
    assert_no_pii(stored.model_dump_json())


async def test_the_request_carries_the_message_not_a_fresh_read() -> None:
    case, _, claim, llm, _ = await build_world()

    await case.execute(build_message(claim))

    request = llm.requests[0]
    assert request.claim_id == claim.id
    assert request.redacted_text == "Patient <PERSON> with M54.5."
    assert [policy.id for policy in request.policies] == [POLICY_ID]
    assert request.prompt_version == "v1"
    assert_no_pii(request.redacted_text)


async def test_found_codes_are_filtered_against_the_seeded_catalogue() -> None:
    # Phase 1 carry-over: the regex matches "B12" in "Vitamin B12". The catalogue is
    # what stops it reaching the prompt as a diagnosis.
    case, _, claim, llm, _ = await build_world()

    await case.execute(build_message(claim, found_codes=["M54.5", "B12", "T12"]))

    assert llm.requests[0].found_codes == ["M54.5", "T12"]


async def test_a_low_confidence_evaluation_reaches_review_pending() -> None:
    llm = FakeLLMGateway(
        evaluation=Evaluation(
            decision=Decision.INSUFFICIENT_EVIDENCE,
            confidence=0.4,
            model="fake",
            prompt_version="v1",
        )
    )
    case, uow, claim, _, webhook = await build_world(llm=llm)

    await case.execute(build_message(claim))

    assert uow.claims.claims[claim.id].status is ClaimStatus.REVIEW_PENDING
    assert webhook.deliveries == []
    assert len(uow.review_tasks.tasks) == 1


async def test_a_transient_llm_error_propagates_and_leaves_the_claim_queued() -> None:
    llm = FakeLLMGateway(error=LLMTransientError("429"))
    case, uow, claim, _, _ = await build_world(llm=llm)

    with pytest.raises(LLMTransientError):
        await case.execute(build_message(claim))

    assert uow.claims.claims[claim.id].status is ClaimStatus.QUEUED
    assert uow.claims.saved == []


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (LLMInvalidOutput("no tool_calls"), "llm_invalid_output"),
        (LLMPermanentError("401"), "llm_permanent_error"),
    ],
)
async def test_a_permanent_llm_failure_fails_the_claim_and_opens_a_review(
    error: Exception, reason: str
) -> None:
    case, uow, claim, _, webhook = await build_world(llm=FakeLLMGateway(error=error))

    await case.execute(build_message(claim))

    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.EVALUATION_FAILED
    assert stored.failure_reason == reason
    tasks = list(uow.review_tasks.tasks.values())
    assert len(tasks) == 1
    assert tasks[0].reason is ReviewReason.EVALUATION_FAILED
    assert webhook.deliveries == []
    assert uow.commits == 1


async def test_a_delivery_failure_still_commits_the_evaluation_once() -> None:
    webhook = FakeWebhookClient(error=WebhookPermanentError("400"))
    case, uow, claim, _, _ = await build_world(webhook=webhook)

    await case.execute(build_message(claim))

    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.NOTIFY_FAILED
    assert stored.failure_reason == "webhook_rejected"
    assert stored.evaluation is not None  # the evaluation survives the failed delivery
    assert uow.commits == 1
    assert_no_pii(stored.model_dump_json())


async def test_a_redelivered_message_for_an_evaluated_claim_calls_no_gateway() -> None:
    claim = build_claim(status=ClaimStatus.EVALUATED)
    case, uow, claim, llm, webhook = await build_world(claim=claim)

    await case.execute(build_message(claim))

    assert llm.requests == []
    assert webhook.deliveries == []
    assert uow.claims.saved == []


async def test_a_message_that_beats_the_api_s_queued_commit_is_requeued(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """UC-01 publishes, then commits `QUEUED`. A worker fast enough to read the claim in
    between sees `POLICIES_ATTACHED`; acking that as a duplicate strands the claim in
    `QUEUED` with no message left (found by the E2E suite)."""
    claim = build_claim(status=ClaimStatus.POLICIES_ATTACHED)
    case, uow, claim, llm, webhook = await build_world(claim=claim)
    holds: list[tuple[float, bool]] = []

    async def spy_sleep(seconds: float) -> None:
        holds.append((seconds, uow.active))

    monkeypatch.setattr("ecet.application.use_cases.evaluate_claim.asyncio.sleep", spy_sleep)

    with pytest.raises(ClaimNotYetQueued):
        await case.execute(build_message(claim))

    # Held once, for the configured delay, with the read unit of work already closed.
    assert holds == [(NOT_YET_QUEUED_DELAY_S, False)]

    assert llm.requests == []
    assert webhook.deliveries == []
    assert uow.claims.saved == []


async def test_a_message_for_an_unknown_claim_is_swallowed() -> None:
    case, _, claim, llm, _ = await build_world()
    orphan = build_message(claim).model_copy(update={"claim_id": ClaimId(uuid4())})

    await case.execute(orphan)  # no exception: the consumer must ack a poison message

    assert llm.requests == []


async def test_a_tenant_mismatch_between_message_and_claim_is_refused() -> None:
    case, uow, claim, llm, _ = await build_world()
    forged = build_message(claim).model_copy(update={"tenant_id": TenantId("tenant-b")})

    await case.execute(forged)

    assert llm.requests == []
    assert uow.claims.claims[claim.id].status is ClaimStatus.QUEUED


async def test_the_vendor_is_called_with_no_transaction_open() -> None:
    """Phase 4 carry-over #11: an `llm_timeout_s=60` call inside the unit of work is a
    Postgres connection idle-in-transaction for a minute, `worker_prefetch` of them
    per worker."""
    claim = build_claim()
    uow = FakeUnitOfWork(tenants=[build_tenant()], known_codes=CATALOGUE)
    await uow.claims.add(claim)
    llm = FakeLLMGateway(watch=uow)
    webhook = FakeWebhookClient(watch=uow)
    case = build_case(uow, llm, webhook)

    await case.execute(build_message(claim))

    assert llm.uow_open_during_call == [False]
    assert webhook.uow_open_during_call == [False]


async def test_the_claim_is_saved_from_the_second_unit_of_work() -> None:
    case, uow, claim, _, _ = await build_world()

    await case.execute(build_message(claim))

    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.APPROVED_AUTO
    assert stored.evaluation is not None
    assert stored.notification_attempts == 1
    assert uow.entries == 2  # one read-only, one write


async def test_a_claim_that_moved_on_while_the_vendor_was_answering_is_left_alone() -> None:
    """The status check is repeated in the second unit of work: two workers can hold
    the same message, and the one that finishes second must not overwrite the first.
    The route was already decided and, here, already delivered by the time that check
    fires — the webhook cannot be un-sent, so the test pins that down rather than
    letting it pass unnoticed (see `evaluate.delivery_discarded`)."""
    case, uow, claim, llm, webhook = await build_world()

    async def advance_it(request: object) -> Evaluation:
        stolen = await uow.claims.get(claim.id)
        stolen.transition(ClaimStatus.EVALUATION_FAILED, reason="llm_permanent_error", now=NOW)
        await uow.claims.save(stolen)
        return llm.evaluation

    llm.evaluate = advance_it  # type: ignore[method-assign]

    await case.execute(build_message(claim))

    assert uow.claims.claims[claim.id].status is ClaimStatus.EVALUATION_FAILED
    assert len(webhook.deliveries) == 1  # the client was already notified before this fired


async def test_a_deactivated_tenant_parks_the_claim_without_a_delivery() -> None:
    """`_tenant` — the one genuinely new I/O-touching helper in the read/call/write
    split — has no coverage otherwise: `RouteDecision.apply` no longer reads the
    tenant itself, so this has to be driven from `EvaluateClaim` to exercise it."""
    claim = build_claim()
    inactive_tenant = build_tenant().model_copy(update={"active": False})
    uow = FakeUnitOfWork(tenants=[inactive_tenant], known_codes=CATALOGUE)
    await uow.claims.add(claim)
    webhook = FakeWebhookClient()
    case = build_case(uow, FakeLLMGateway(), webhook)

    await case.execute(build_message(claim))

    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.NOTIFY_FAILED
    assert stored.failure_reason == "tenant_inactive"
    assert stored.notification_attempts == 0  # nothing left the process
    assert webhook.deliveries == []
