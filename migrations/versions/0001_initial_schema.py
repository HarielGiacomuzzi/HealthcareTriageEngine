"""initial schema

Revision ID: 0001_initial
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: None = None
depends_on: None = None


def upgrade() -> None:
    op.create_table(
        "tenants",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column("webhook_secret", sa.Text(), nullable=False),
        sa.Column("active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "icd10_codes",
        sa.Column("code", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("code"),
    )
    op.create_table(
        "policies",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("covered_codes", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("excluded_codes", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("criteria_text", sa.Text(), nullable=False),
        sa.Column("required_evidence", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=False),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "name", "version", name="uq_policies_tenant_name_version"),
    )
    op.create_index("ix_policies_tenant_active", "policies", ["tenant_id", "active"])
    op.create_table(
        "claims",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("bucket", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("etag", sa.Text(), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("redacted_text", sa.Text(), nullable=True),
        sa.Column("entity_counts", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("redactor", sa.Text(), nullable=True),
        sa.Column("policy_ids", postgresql.ARRAY(sa.Uuid()), nullable=False),
        sa.Column("deterministic", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("evaluation", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column(
            "notification_attempts", sa.Integer(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column("last_notify_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("bucket", "key", "etag", name="uq_claims_source_object"),
    )
    op.create_index("ix_claims_tenant_status", "claims", ["tenant_id", "status"])
    op.create_table(
        "review_tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("claim_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("resolution", sa.Text(), nullable=True),
        sa.Column("reviewer", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["claim_id"], ["claims.id"]),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("claim_id"),
    )
    op.create_index("ix_review_tasks_tenant_status", "review_tasks", ["tenant_id", "status"])


def downgrade() -> None:
    op.drop_index("ix_review_tasks_tenant_status", table_name="review_tasks")
    op.drop_table("review_tasks")
    op.drop_index("ix_claims_tenant_status", table_name="claims")
    op.drop_table("claims")
    op.drop_index("ix_policies_tenant_active", table_name="policies")
    op.drop_table("policies")
    op.drop_table("icd10_codes")
    op.drop_table("tenants")
