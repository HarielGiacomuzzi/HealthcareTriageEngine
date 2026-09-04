# UC-07 RouteDecision ([ADR-003](../00-overview.md#4-adrs))

## Input
`claim: Claim` with `evaluation` set.

## Steps
1. `route = triage(claim.evaluation, threshold)` (domain pure function).
2. `AUTO_NOTIFY` → [UC-08](UC-08-notify-client.md) NotifyClient(claim, outcome=evaluation.decision, source="auto").
   Success → transition `APPROVED_AUTO`. Failure after retries → `NOTIFY_FAILED` with reason.
   No review task created for delivery failures; operator retries via `POST /v1/claims/{id}/retry-notify`.
3. `HUMAN_REVIEW` → [UC-09](UC-09-human-review.md) RequestHumanReview(reason = `LOW_CONFIDENCE`, or
   `DETERMINISTIC_UNCERTAIN_LLM_LOW` if deterministic verdict was UNCERTAIN). Transition `REVIEW_PENDING`.
4. Save, commit. Emit metric `triage_route_total{route}`.

Threshold from config `ECET_CONFIDENCE_THRESHOLD` (default 0.85). Injected, not read inside domain.

## Tests
- 0.90 → notify called, status APPROVED_AUTO.
- 0.80 → review task created, status REVIEW_PENDING, notify not called.
- Notify raises after retries → NOTIFY_FAILED, reason stored.
