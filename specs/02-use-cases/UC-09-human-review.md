# UC-09 RequestHumanReview & ResolveReview

## UC-09a RequestHumanReview
Input: `(claim, reason: ReviewReason)`.
1. Create `ReviewTask(status=OPEN)`; `review_tasks.add`.
2. Caller transitions claim → `REVIEW_PENDING`.
Idempotent: existing OPEN task for claim → return it, no duplicate.

## UC-09b ListOpenReviews
Input: `(tenant_id, limit)` → `list[ReviewTaskView]` including claim redacted text, deterministic checks, LLM evaluation (if any). Tenant-scoped only.

## UC-09c ResolveReview
Input:
```python
class ResolveReviewCommand(BaseModel):
    task_id: UUID
    tenant_id: TenantId
    reviewer: str
    resolution: Literal["MEETS_NECESSITY", "DOES_NOT_MEET"]
    notes: str | None
```
1. Load task; must be OPEN and belong to tenant → else `ReviewTaskNotFound` / `ReviewAlreadyResolved`.
2. Set resolution, reviewer, `resolved_at`; status `RESOLVED`.
3. [UC-08](UC-08-notify-client.md) NotifyClient with `decided_by="human"`, `confidence=1.0`.
4. Claim → `REVIEW_RESOLVED` (or `NOTIFY_FAILED` on delivery failure).
5. Commit.

## Tests
- Duplicate request → one task.
- Resolve twice → error.
- Resolve triggers notify with `decided_by="human"`.
- Cross-tenant resolve → not found.
