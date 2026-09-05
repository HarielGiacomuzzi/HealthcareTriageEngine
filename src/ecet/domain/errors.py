"""Domain errors. Everything the domain can refuse to do raises one of these."""


class DomainError(Exception):
    """Base class for every domain-level failure."""


class InvalidObjectKey(DomainError):
    """Object key does not match `tenants/{tenant_id}/claims/{name}.pdf`."""


class InvalidTransition(DomainError):
    """Claim status transition is not allowed from the current status."""


class TenantMismatch(DomainError):
    """Claim `tenant_id` disagrees with the tenant segment of its source object key."""


class InvalidReviewResolution(DomainError):
    """A human resolution must be `MEETS_NECESSITY` or `DOES_NOT_MEET`."""


class PdfTooLarge(DomainError):
    """Source object is larger than the configured `max_pdf_bytes`."""


class NoPoliciesForTenant(DomainError):
    """Tenant has no active, effective policy (ADR-005 — hard failure, no LLM call)."""


class ClaimNotFound(DomainError):
    """No claim with that id."""


class TenantNotFound(DomainError):
    """No active tenant with that id."""


class ReviewTaskNotFound(DomainError):
    """No review task with that id for that tenant."""


class ReviewAlreadyResolved(DomainError):
    """Review task has already been resolved."""


class ConcurrentModification(DomainError):
    """Optimistic save lost: the row changed since it was read."""
