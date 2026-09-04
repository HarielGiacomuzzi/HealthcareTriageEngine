# Use Cases

Module: `ecet/application/use_cases/`. One class per use case, `async def execute(self, cmd) -> result`.
Constructor takes ports only (repositories, gateways). No framework imports.

| ID    | Name                     | Trigger                | Runs in |
|-------|--------------------------|------------------------|---------|
| [UC-01](UC-01-ingest-claim-document.md) | IngestClaimDocument      | S3 event               | api     |
| [UC-02](UC-02-redact-pii.md) | RedactPii                | called by UC-01        | api     |
| [UC-03](UC-03-attach-tenant-policies.md) | AttachTenantPolicies     | called by UC-01        | api     |
| [UC-04](UC-04-run-deterministic-checks.md) | RunDeterministicChecks   | called by UC-01        | api     |
| [UC-05](UC-05-enqueue-evaluation.md) | EnqueueEvaluation        | called by UC-01        | api     |
| [UC-06](UC-06-evaluate-claim.md) | EvaluateClaim            | queue message          | worker  |
| [UC-07](UC-07-route-decision.md) | RouteDecision            | called by UC-06        | worker  |
| [UC-08](UC-08-notify-client.md) | NotifyClient             | called by UC-07 / [UC-09](UC-09-human-review.md) resolve | worker / api |
| UC-09 | RequestHumanReview + ResolveReview | UC-07 / REST | worker / api |

UC-01 is orchestrator for the ingestion side; UC-06 for the worker side.
Sub use cases are separate classes so each is unit-testable with fakes.

## Ports defined in `ecet/application/ports/`

| Port               | Methods                                                |
|--------------------|--------------------------------------------------------|
| `ObjectStorage`    | `get_bytes(bucket, key) -> bytes`                      |
| `TextExtractor`    | `extract(pdf: bytes) -> str`                           |
| `PiiRedactor`      | `redact(text) -> RedactedText`                         |
| `EvaluationQueue`  | `publish(msg: EvaluationMessage)`                      |
| `LLMGateway`       | `evaluate(req: EvaluationRequest) -> Evaluation`       |
| `WebhookClient`    | `deliver(tenant, payload: ClientNotification) -> None` |
| `Clock`            | `now() -> datetime`                                    |
| `UnitOfWork`       | async context; exposes repos; commit/rollback          |

Domain repository ports live in `ecet/domain/ports/` — see [claim](../01-domain/claim.md#4-repository-port-ecetdomainportsclaim_repositorypy), [policy](../01-domain/policy.md#3-repository-port-ecetdomainportspolicy_repositorypy), [tenant](../01-domain/tenant.md#2-port), [evaluation](../01-domain/evaluation.md#4-reviewtask-human-in-the-loop).
