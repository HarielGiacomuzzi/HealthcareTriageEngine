"""The one place domain models and SQLAlchemy rows meet.

Pure functions, no session, no I/O — so the whole mapping layer is unit-tested
without Docker. `*_to_row_values` returns a plain dict so the repositories can feed
it to `insert()` / `update()` as well as to a row constructor.
"""

from typing import Any

from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.evaluation import (
    Decision,
    DeterministicResult,
    Evaluation,
    ReviewReason,
    ReviewStatus,
    ReviewTask,
)
from ecet.domain.ids import ClaimId, PolicyId, TenantId
from ecet.domain.policy import Icd10Code, Policy
from ecet.domain.tenant import Tenant
from ecet.infrastructure.postgres.orm import ClaimRow, PolicyRow, ReviewTaskRow, TenantRow


def tenant_to_row_values(tenant: Tenant) -> dict[str, Any]:
    return {
        "id": str(tenant.id),
        "name": tenant.name,
        "webhook_url": str(tenant.webhook_url),
        # `model_dump` would write "**********" here.
        "webhook_secret": tenant.webhook_secret.get_secret_value(),
        "active": tenant.active,
    }


def tenant_from_row(row: TenantRow) -> Tenant:
    return Tenant(
        id=TenantId(row.id),
        name=row.name,
        webhook_url=row.webhook_url,  # type: ignore[arg-type]  # pydantic parses the str
        webhook_secret=row.webhook_secret,  # type: ignore[arg-type]  # pydantic wraps it
        active=row.active,
    )


def policy_to_row_values(policy: Policy) -> dict[str, Any]:
    return {
        "id": policy.id,
        "tenant_id": str(policy.tenant_id),
        "name": policy.name,
        "version": policy.version,
        # Sorted so a row diff means a real change, not set iteration order.
        "covered_codes": sorted(code.code for code in policy.covered_codes),
        "excluded_codes": sorted(code.code for code in policy.excluded_codes),
        "criteria_text": policy.criteria_text,
        "required_evidence": list(policy.required_evidence),
        "active": policy.active,
        "effective_from": policy.effective_from,
        "effective_to": policy.effective_to,
    }


def policy_from_row(row: PolicyRow) -> Policy:
    return Policy(
        id=PolicyId(row.id),
        tenant_id=TenantId(row.tenant_id),
        name=row.name,
        version=row.version,
        covered_codes={Icd10Code(code=code) for code in row.covered_codes},
        excluded_codes={Icd10Code(code=code) for code in row.excluded_codes},
        criteria_text=row.criteria_text,
        required_evidence=list(row.required_evidence),
        active=row.active,
        effective_from=row.effective_from,
        effective_to=row.effective_to,
    )


def claim_to_row_values(claim: Claim) -> dict[str, Any]:
    redacted = claim.redacted
    return {
        "id": claim.id,
        "tenant_id": str(claim.tenant_id),
        "bucket": claim.source.bucket,
        "key": claim.source.key,
        "etag": claim.source.etag,
        "size": claim.source.size,
        "status": claim.status.value,
        "redacted_text": None if redacted is None else redacted.text,
        "entity_counts": None if redacted is None else dict(redacted.entity_counts),
        "redactor": None if redacted is None else redacted.redactor,
        "policy_ids": list(claim.policy_ids),
        "deterministic": (
            None if claim.deterministic is None else claim.deterministic.model_dump(mode="json")
        ),
        "evaluation": (
            None if claim.evaluation is None else claim.evaluation.model_dump(mode="json")
        ),
        "failure_reason": claim.failure_reason,
        "notification_attempts": claim.notification_attempts,
        "last_notify_error": claim.last_notify_error,
        "created_at": claim.created_at,
        "updated_at": claim.updated_at,
    }


def claim_from_row(row: ClaimRow) -> Claim:
    # `redactor` is the marker, not `redacted_text`: an all-PII note redacts to "".
    redacted = (
        None
        if row.redactor is None
        else RedactedText(
            text=row.redacted_text or "",
            entity_counts=row.entity_counts or {},
            redactor=row.redactor,
        )
    )
    return Claim(
        id=ClaimId(row.id),
        tenant_id=TenantId(row.tenant_id),
        source=SourceObject(bucket=row.bucket, key=row.key, etag=row.etag, size=row.size),
        status=ClaimStatus(row.status),
        redacted=redacted,
        policy_ids=[PolicyId(policy_id) for policy_id in row.policy_ids],
        deterministic=(
            None
            if row.deterministic is None
            else DeterministicResult.model_validate(row.deterministic)
        ),
        evaluation=(None if row.evaluation is None else Evaluation.model_validate(row.evaluation)),
        failure_reason=row.failure_reason,
        notification_attempts=row.notification_attempts,
        last_notify_error=row.last_notify_error,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def review_task_to_row_values(task: ReviewTask) -> dict[str, Any]:
    return {
        "id": task.id,
        "claim_id": task.claim_id,
        "tenant_id": str(task.tenant_id),
        "reason": task.reason.value,
        "status": task.status.value,
        "resolution": None if task.resolution is None else task.resolution.value,
        "reviewer": task.reviewer,
        "notes": task.notes,
        "created_at": task.created_at,
        "resolved_at": task.resolved_at,
    }


def review_task_from_row(row: ReviewTaskRow) -> ReviewTask:
    return ReviewTask(
        id=row.id,
        claim_id=ClaimId(row.claim_id),
        tenant_id=TenantId(row.tenant_id),
        reason=ReviewReason(row.reason),
        status=ReviewStatus(row.status),
        resolution=None if row.resolution is None else Decision(row.resolution),  # type: ignore[arg-type]
        reviewer=row.reviewer,
        notes=row.notes,
        created_at=row.created_at,
        resolved_at=row.resolved_at,
    )
