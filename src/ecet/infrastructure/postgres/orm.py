"""SQLAlchemy row classes. These are *rows*, never domain models — `mappers.py` is
the only place the two meet.
"""

from datetime import date, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TenantRow(Base):
    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    webhook_url: Mapped[str] = mapped_column(Text, nullable=False)
    webhook_secret: Mapped[str] = mapped_column(Text, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))


class Icd10CodeRow(Base):
    """Reference catalogue: what a real ICD-10 code looks like, seeded per environment."""

    __tablename__ = "icd10_codes"

    code: Mapped[str] = mapped_column(Text, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)


class PolicyRow(Base):
    __tablename__ = "policies"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", "version", name="uq_policies_tenant_name_version"),
        Index("ix_policies_tenant_active", "tenant_id", "active"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    tenant_id: Mapped[str] = mapped_column(Text, ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    covered_codes: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    excluded_codes: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    criteria_text: Mapped[str] = mapped_column(Text, nullable=False)
    required_evidence: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)


class ClaimRow(Base):
    __tablename__ = "claims"
    __table_args__ = (
        # ADR-006 idempotency.
        UniqueConstraint("bucket", "key", "etag", name="uq_claims_source_object"),
        Index("ix_claims_tenant_status", "tenant_id", "status"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    tenant_id: Mapped[str] = mapped_column(Text, ForeignKey("tenants.id"), nullable=False)
    bucket: Mapped[str] = mapped_column(Text, nullable=False)
    key: Mapped[str] = mapped_column(Text, nullable=False)
    etag: Mapped[str] = mapped_column(Text, nullable=False)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    redacted_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    entity_counts: Mapped[dict[str, int] | None] = mapped_column(JSONB, nullable=True)
    redactor: Mapped[str | None] = mapped_column(Text, nullable=True)
    policy_ids: Mapped[list[UUID]] = mapped_column(ARRAY(Uuid), nullable=False)
    deterministic: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    evaluation: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    notification_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    last_notify_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ReviewTaskRow(Base):
    __tablename__ = "review_tasks"
    __table_args__ = (Index("ix_review_tasks_tenant_status", "tenant_id", "status"),)

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    # Unique: UC-09a must return the existing OPEN task instead of creating a second one.
    claim_id: Mapped[UUID] = mapped_column(
        Uuid, ForeignKey("claims.id"), nullable=False, unique=True
    )
    tenant_id: Mapped[str] = mapped_column(Text, ForeignKey("tenants.id"), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    resolution: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewer: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
