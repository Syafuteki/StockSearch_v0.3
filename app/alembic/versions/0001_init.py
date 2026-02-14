"""initial schema

Revision ID: 0001_init
Revises: 
Create Date: 2026-02-14 00:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "instruments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("code", sa.String(length=16), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("market", sa.String(length=64), nullable=True),
        sa.Column("issued_shares", sa.Integer(), nullable=True),
        sa.Column("market_cap", sa.Numeric(20, 2), nullable=True),
        sa.Column("raw_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("as_of_date", "code", name="uq_instruments_date_code"),
    )
    op.create_index("ix_instruments_as_of_date", "instruments", ["as_of_date"])
    op.create_index("ix_instruments_code", "instruments", ["code"])

    op.create_table(
        "daily_bars",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("code", sa.String(length=16), nullable=False),
        sa.Column("open", sa.Numeric(20, 6), nullable=True),
        sa.Column("high", sa.Numeric(20, 6), nullable=True),
        sa.Column("low", sa.Numeric(20, 6), nullable=True),
        sa.Column("close", sa.Numeric(20, 6), nullable=True),
        sa.Column("adj_close", sa.Numeric(20, 6), nullable=True),
        sa.Column("volume", sa.Integer(), nullable=True),
        sa.Column("market_cap", sa.Numeric(20, 2), nullable=True),
        sa.Column("raw_json", sa.JSON(), nullable=False),
        sa.UniqueConstraint("trade_date", "code", name="uq_daily_bars_date_code"),
    )
    op.create_index("ix_daily_bars_trade_date", "daily_bars", ["trade_date"])
    op.create_index("ix_daily_bars_code", "daily_bars", ["code"])

    op.create_table(
        "features_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("code", sa.String(length=16), nullable=False),
        sa.Column("ma10", sa.Float(), nullable=True),
        sa.Column("ma25", sa.Float(), nullable=True),
        sa.Column("ma75", sa.Float(), nullable=True),
        sa.Column("ma75_slope_5", sa.Float(), nullable=True),
        sa.Column("roc20", sa.Float(), nullable=True),
        sa.Column("roc60", sa.Float(), nullable=True),
        sa.Column("rsi14", sa.Float(), nullable=True),
        sa.Column("atr14", sa.Float(), nullable=True),
        sa.Column("volume_ratio20", sa.Float(), nullable=True),
        sa.Column("breakout_strength20", sa.Float(), nullable=True),
        sa.Column("volatility_penalty", sa.Float(), nullable=True),
        sa.Column("raw_json", sa.JSON(), nullable=False),
        sa.UniqueConstraint("trade_date", "code", name="uq_features_daily_date_code"),
    )
    op.create_index("ix_features_daily_trade_date", "features_daily", ["trade_date"])
    op.create_index("ix_features_daily_code", "features_daily", ["code"])

    op.create_table(
        "universe_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("code", sa.String(length=16), nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("market_cap", sa.Numeric(20, 2), nullable=True),
        sa.Column("market_cap_estimated", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("rule_version", sa.String(length=64), nullable=False),
        sa.UniqueConstraint("trade_date", "code", "rule_version", name="uq_universe_daily"),
    )
    op.create_index("ix_universe_daily_trade_date", "universe_daily", ["trade_date"])
    op.create_index("ix_universe_daily_code", "universe_daily", ["code"])
    op.create_index("ix_universe_daily_rule_version", "universe_daily", ["rule_version"])

    op.create_table(
        "screen_top30_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("code", sa.String(length=16), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("score_breakdown", sa.JSON(), nullable=False),
        sa.Column("rule_version", sa.String(length=64), nullable=False),
        sa.UniqueConstraint("trade_date", "code", "rule_version", name="uq_screen_top30_daily"),
        sa.UniqueConstraint("trade_date", "rank", "rule_version", name="uq_screen_top30_rank"),
    )
    op.create_index("ix_screen_top30_daily_trade_date", "screen_top30_daily", ["trade_date"])
    op.create_index("ix_screen_top30_daily_code", "screen_top30_daily", ["code"])
    op.create_index("ix_screen_top30_daily_rule_version", "screen_top30_daily", ["rule_version"])

    op.create_table(
        "shortlist_top10_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("code", sa.String(length=16), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("llm_run_id", sa.Integer(), nullable=True),
        sa.Column("reason_json", sa.JSON(), nullable=False),
        sa.Column("rule_version", sa.String(length=64), nullable=False),
        sa.UniqueConstraint("trade_date", "code", "rule_version", name="uq_shortlist_top10_daily"),
        sa.UniqueConstraint("trade_date", "rank", "rule_version", name="uq_shortlist_top10_rank"),
    )
    op.create_index("ix_shortlist_top10_daily_trade_date", "shortlist_top10_daily", ["trade_date"])
    op.create_index("ix_shortlist_top10_daily_code", "shortlist_top10_daily", ["code"])
    op.create_index("ix_shortlist_top10_daily_rule_version", "shortlist_top10_daily", ["rule_version"])
    op.create_index("ix_shortlist_top10_daily_llm_run_id", "shortlist_top10_daily", ["llm_run_id"])

    op.create_table(
        "market_context_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("trade_date", sa.Date(), nullable=False, unique=True),
        sa.Column("sq_week_flag", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("context_json", sa.JSON(), nullable=False),
        sa.Column("raw_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_market_context_daily_trade_date", "market_context_daily", ["trade_date"])

    op.create_table(
        "events_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("code", sa.String(length=16), nullable=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_events_daily_trade_date", "events_daily", ["trade_date"])
    op.create_index("ix_events_daily_code", "events_daily", ["code"])
    op.create_index("ix_events_daily_event_type", "events_daily", ["event_type"])

    op.create_table(
        "llm_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("report_date", sa.Date(), nullable=False),
        sa.Column("run_type", sa.String(length=32), nullable=False),
        sa.Column("model", sa.String(length=128), nullable=False),
        sa.Column("temperature", sa.Float(), nullable=False),
        sa.Column("prompt_json", sa.JSON(), nullable=False),
        sa.Column("output_json", sa.JSON(), nullable=False),
        sa.Column("validation_ok", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("validation_errors", sa.Text(), nullable=True),
        sa.Column("token_usage_json", sa.JSON(), nullable=True),
    )
    op.create_index("ix_llm_runs_run_at", "llm_runs", ["run_at"])
    op.create_index("ix_llm_runs_report_date", "llm_runs", ["report_date"])
    op.create_index("ix_llm_runs_run_type", "llm_runs", ["run_type"])

    op.create_table(
        "notifications",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("report_date", sa.Date(), nullable=False),
        sa.Column("run_type", sa.String(length=32), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("success", sa.Boolean(), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.create_index("ix_notifications_sent_at", "notifications", ["sent_at"])
    op.create_index("ix_notifications_report_date", "notifications", ["report_date"])
    op.create_index("ix_notifications_run_type", "notifications", ["run_type"])

    op.create_table(
        "rule_versions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("version", sa.String(length=64), nullable=False, unique=True),
        sa.Column("applied_from", sa.Date(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_rule_versions_version", "rule_versions", ["version"])
    op.create_index("ix_rule_versions_applied_from", "rule_versions", ["applied_from"])

    op.create_table(
        "rule_suggestions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("report_date", sa.Date(), nullable=False),
        sa.Column("code", sa.String(length=16), nullable=True),
        sa.Column("suggestion_text", sa.Text(), nullable=False),
        sa.Column("source_llm_run_id", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("reason_memo", sa.Text(), nullable=True),
        sa.Column("raw_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_rule_suggestions_report_date", "rule_suggestions", ["report_date"])
    op.create_index("ix_rule_suggestions_code", "rule_suggestions", ["code"])
    op.create_index("ix_rule_suggestions_source_llm_run_id", "rule_suggestions", ["source_llm_run_id"])


def downgrade() -> None:
    for name in [
        "rule_suggestions",
        "rule_versions",
        "notifications",
        "llm_runs",
        "events_daily",
        "market_context_daily",
        "shortlist_top10_daily",
        "screen_top30_daily",
        "universe_daily",
        "features_daily",
        "daily_bars",
        "instruments",
    ]:
        op.drop_table(name)

