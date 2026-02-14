"""fund intel theme rag tables

Revision ID: 0002_fund_intel_theme_rag
Revises: 0001_init
Create Date: 2026-02-14 00:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0002_fund_intel_theme_rag"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "fund_universe_state",
        sa.Column("code", sa.String(length=16), primary_key=True),
        sa.Column("state", sa.Enum("IN", "WATCH", "OUT", name="fund_state_enum", native_enum=False), nullable=False),
        sa.Column("fund_score", sa.Float(), nullable=True),
        sa.Column("risk_hard", sa.JSON(), nullable=False),
        sa.Column("risk_soft", sa.JSON(), nullable=False),
        sa.Column("tags", sa.JSON(), nullable=False),
        sa.Column("thesis_bull", sa.Text(), nullable=True),
        sa.Column("thesis_bear", sa.Text(), nullable=True),
        sa.Column("evidence_refs", sa.JSON(), nullable=False),
        sa.Column("data_gaps", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_fund_universe_state_state", "fund_universe_state", ["state"])
    op.create_index("ix_fund_universe_state_updated_at", "fund_universe_state", ["updated_at"])

    op.create_table(
        "fund_features_snapshot",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(length=16), nullable=False),
        sa.Column("asof_date", sa.Date(), nullable=False),
        sa.Column("features", sa.JSON(), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("code", "asof_date", name="uq_fund_features_snapshot"),
    )
    op.create_index("ix_fund_features_snapshot_code", "fund_features_snapshot", ["code"])
    op.create_index("ix_fund_features_snapshot_asof_date", "fund_features_snapshot", ["asof_date"])

    op.create_table(
        "intel_queue",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column(
            "session",
            sa.Enum("morning", "close", name="intel_session_enum", native_enum=False),
            nullable=False,
        ),
        sa.Column("code", sa.String(length=16), nullable=False),
        sa.Column("priority", sa.Float(), nullable=False),
        sa.Column("sources_seed", sa.JSON(), nullable=False),
        sa.Column(
            "status",
            sa.Enum("pending", "done", "skipped", "failed", name="intel_queue_status_enum", native_enum=False),
            nullable=False,
        ),
        sa.Column("idempotency_key", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("business_date", "session", "code", name="uq_intel_queue_day_session_code"),
        sa.UniqueConstraint("idempotency_key"),
    )
    op.create_index("ix_intel_queue_business_date", "intel_queue", ["business_date"])
    op.create_index("ix_intel_queue_session", "intel_queue", ["session"])
    op.create_index("ix_intel_queue_code", "intel_queue", ["code"])
    op.create_index("ix_intel_queue_status", "intel_queue", ["status"])
    op.create_index("ix_intel_queue_idempotency_key", "intel_queue", ["idempotency_key"])

    op.create_table(
        "intel_items",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(length=16), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_type", sa.String(length=64), nullable=False),
        sa.Column("headline", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("facts", sa.JSON(), nullable=False),
        sa.Column("tags", sa.JSON(), nullable=False),
        sa.Column("risk_flags", sa.JSON(), nullable=False),
        sa.Column("critical_risk", sa.Boolean(), nullable=False),
        sa.Column("evidence_refs", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_intel_items_code", "intel_items", ["code"])
    op.create_index("ix_intel_items_published_at", "intel_items", ["published_at"])
    op.create_index("ix_intel_items_source_type", "intel_items", ["source_type"])
    op.create_index("ix_intel_items_critical_risk", "intel_items", ["critical_risk"])
    op.create_index("ix_intel_items_created_at", "intel_items", ["created_at"])

    op.create_table(
        "intel_daily_budget",
        sa.Column("business_date", sa.Date(), primary_key=True),
        sa.Column("done_count", sa.Integer(), nullable=False),
        sa.Column("morning_done", sa.Integer(), nullable=False),
        sa.Column("close_done", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "themes",
        sa.Column("theme_id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=128), nullable=False, unique=True),
        sa.Column("keywords", sa.JSON(), nullable=False),
        sa.Column("allowed_sources", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_themes_name", "themes", ["name"])

    op.create_table(
        "theme_strength_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("theme_id", sa.Integer(), nullable=False),
        sa.Column("asof_date", sa.Date(), nullable=False),
        sa.Column("strength", sa.Float(), nullable=False),
        sa.Column("drivers", sa.JSON(), nullable=False),
        sa.UniqueConstraint("theme_id", "asof_date", name="uq_theme_strength_daily"),
    )
    op.create_index("ix_theme_strength_daily_theme_id", "theme_strength_daily", ["theme_id"])
    op.create_index("ix_theme_strength_daily_asof_date", "theme_strength_daily", ["asof_date"])

    op.create_table(
        "theme_symbol_map",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("theme_id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=16), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("rationale", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("theme_id", "code", name="uq_theme_symbol_map"),
    )
    op.create_index("ix_theme_symbol_map_theme_id", "theme_symbol_map", ["theme_id"])
    op.create_index("ix_theme_symbol_map_code", "theme_symbol_map", ["code"])

    op.create_table(
        "kb_documents",
        sa.Column("doc_id", sa.String(length=128), primary_key=True),
        sa.Column("source_type", sa.String(length=64), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("tags", sa.JSON(), nullable=False),
        sa.Column("source_id", sa.String(length=255), nullable=True),
        sa.Column("rights", sa.String(length=64), nullable=True),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_kb_documents_source_type", "kb_documents", ["source_type"])
    op.create_index("ix_kb_documents_source_id", "kb_documents", ["source_id"])
    op.create_index("ix_kb_documents_sha256", "kb_documents", ["sha256"])

    op.create_table(
        "kb_chunks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("doc_id", sa.String(length=128), nullable=False),
        sa.Column("chunk_id", sa.Integer(), nullable=False),
        sa.Column("loc", sa.String(length=128), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("embedding", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("doc_id", "chunk_id", name="uq_kb_chunk"),
    )
    op.create_index("ix_kb_chunks_doc_id", "kb_chunks", ["doc_id"])

    op.create_table(
        "kb_approvals",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("item_id", sa.String(length=128), nullable=False),
        sa.Column("item_type", sa.String(length=32), nullable=False),
        sa.Column(
            "status",
            sa.Enum("draft", "approved", "rejected", name="approval_status_enum", native_enum=False),
            nullable=False,
        ),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.UniqueConstraint("item_id", "item_type", name="uq_kb_approval_item"),
    )
    op.create_index("ix_kb_approvals_item_id", "kb_approvals", ["item_id"])
    op.create_index("ix_kb_approvals_item_type", "kb_approvals", ["item_type"])
    op.create_index("ix_kb_approvals_status", "kb_approvals", ["status"])

    op.create_table(
        "fund_rule_suggestions",
        sa.Column("proposal_id", sa.Integer(), primary_key=True),
        sa.Column("scope", sa.String(length=64), nullable=False),
        sa.Column("diff", sa.JSON(), nullable=False),
        sa.Column("why", sa.Text(), nullable=False),
        sa.Column("risk", sa.Text(), nullable=True),
        sa.Column("expected_effect", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_fund_rule_suggestions_scope", "fund_rule_suggestions", ["scope"])
    op.create_index("ix_fund_rule_suggestions_created_at", "fund_rule_suggestions", ["created_at"])

    op.create_table(
        "intel_rule_suggestions",
        sa.Column("proposal_id", sa.Integer(), primary_key=True),
        sa.Column("scope", sa.String(length=64), nullable=False),
        sa.Column("diff", sa.JSON(), nullable=False),
        sa.Column("why", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_intel_rule_suggestions_scope", "intel_rule_suggestions", ["scope"])
    op.create_index("ix_intel_rule_suggestions_created_at", "intel_rule_suggestions", ["created_at"])


def downgrade() -> None:
    for name in [
        "intel_rule_suggestions",
        "fund_rule_suggestions",
        "kb_approvals",
        "kb_chunks",
        "kb_documents",
        "theme_symbol_map",
        "theme_strength_daily",
        "themes",
        "intel_daily_budget",
        "intel_items",
        "intel_queue",
        "fund_features_snapshot",
        "fund_universe_state",
    ]:
        op.drop_table(name)

