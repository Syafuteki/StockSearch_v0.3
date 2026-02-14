from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Enum,
    Float,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
try:
    from pgvector.sqlalchemy import Vector
except Exception:  # pragma: no cover
    Vector = None


class Base(DeclarativeBase):
    pass


class Instrument(Base):
    __tablename__ = "instruments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    as_of_date: Mapped[date] = mapped_column(Date, index=True)
    code: Mapped[str] = mapped_column(String(16), index=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    market: Mapped[str | None] = mapped_column(String(64), nullable=True)
    issued_shares: Mapped[int | None] = mapped_column(Integer, nullable=True)
    market_cap: Mapped[float | None] = mapped_column(Numeric(20, 2), nullable=True)
    raw_json: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (UniqueConstraint("as_of_date", "code", name="uq_instruments_date_code"),)


class DailyBar(Base):
    __tablename__ = "daily_bars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    code: Mapped[str] = mapped_column(String(16), index=True)
    open: Mapped[float | None] = mapped_column(Numeric(20, 6), nullable=True)
    high: Mapped[float | None] = mapped_column(Numeric(20, 6), nullable=True)
    low: Mapped[float | None] = mapped_column(Numeric(20, 6), nullable=True)
    close: Mapped[float | None] = mapped_column(Numeric(20, 6), nullable=True)
    adj_close: Mapped[float | None] = mapped_column(Numeric(20, 6), nullable=True)
    volume: Mapped[int | None] = mapped_column(Integer, nullable=True)
    market_cap: Mapped[float | None] = mapped_column(Numeric(20, 2), nullable=True)
    raw_json: Mapped[dict] = mapped_column(JSON)

    __table_args__ = (UniqueConstraint("trade_date", "code", name="uq_daily_bars_date_code"),)


class FeaturesDaily(Base):
    __tablename__ = "features_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    code: Mapped[str] = mapped_column(String(16), index=True)
    ma10: Mapped[float | None] = mapped_column(Float, nullable=True)
    ma25: Mapped[float | None] = mapped_column(Float, nullable=True)
    ma75: Mapped[float | None] = mapped_column(Float, nullable=True)
    ma75_slope_5: Mapped[float | None] = mapped_column(Float, nullable=True)
    roc20: Mapped[float | None] = mapped_column(Float, nullable=True)
    roc60: Mapped[float | None] = mapped_column(Float, nullable=True)
    rsi14: Mapped[float | None] = mapped_column(Float, nullable=True)
    atr14: Mapped[float | None] = mapped_column(Float, nullable=True)
    volume_ratio20: Mapped[float | None] = mapped_column(Float, nullable=True)
    breakout_strength20: Mapped[float | None] = mapped_column(Float, nullable=True)
    volatility_penalty: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_json: Mapped[dict] = mapped_column(JSON)

    __table_args__ = (UniqueConstraint("trade_date", "code", name="uq_features_daily_date_code"),)


class UniverseDaily(Base):
    __tablename__ = "universe_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    code: Mapped[str] = mapped_column(String(16), index=True)
    passed: Mapped[bool] = mapped_column(Boolean, default=True)
    market_cap: Mapped[float | None] = mapped_column(Numeric(20, 2), nullable=True)
    market_cap_estimated: Mapped[bool] = mapped_column(Boolean, default=False)
    details_json: Mapped[dict] = mapped_column(JSON)
    rule_version: Mapped[str] = mapped_column(String(64), index=True)

    __table_args__ = (UniqueConstraint("trade_date", "code", "rule_version", name="uq_universe_daily"),)


class ScreenTop30Daily(Base):
    __tablename__ = "screen_top30_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    code: Mapped[str] = mapped_column(String(16), index=True)
    rank: Mapped[int] = mapped_column(Integer)
    score: Mapped[float] = mapped_column(Float)
    score_breakdown: Mapped[dict] = mapped_column(JSON)
    rule_version: Mapped[str] = mapped_column(String(64), index=True)

    __table_args__ = (
        UniqueConstraint("trade_date", "code", "rule_version", name="uq_screen_top30_daily"),
        UniqueConstraint("trade_date", "rank", "rule_version", name="uq_screen_top30_rank"),
    )


class ShortlistTop10Daily(Base):
    __tablename__ = "shortlist_top10_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    code: Mapped[str] = mapped_column(String(16), index=True)
    rank: Mapped[int] = mapped_column(Integer)
    llm_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    reason_json: Mapped[dict] = mapped_column(JSON)
    rule_version: Mapped[str] = mapped_column(String(64), index=True)

    __table_args__ = (
        UniqueConstraint("trade_date", "code", "rule_version", name="uq_shortlist_top10_daily"),
        UniqueConstraint("trade_date", "rank", "rule_version", name="uq_shortlist_top10_rank"),
    )


class MarketContextDaily(Base):
    __tablename__ = "market_context_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, index=True, unique=True)
    sq_week_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    context_json: Mapped[dict] = mapped_column(JSON)
    raw_json: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class EventsDaily(Base):
    __tablename__ = "events_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    code: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    event_type: Mapped[str] = mapped_column(String(64), index=True)
    payload_json: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class LlmRun(Base):
    __tablename__ = "llm_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    report_date: Mapped[date] = mapped_column(Date, index=True)
    run_type: Mapped[str] = mapped_column(String(32), index=True)
    model: Mapped[str] = mapped_column(String(128))
    temperature: Mapped[float] = mapped_column(Float)
    prompt_json: Mapped[dict] = mapped_column(JSON)
    output_json: Mapped[dict] = mapped_column(JSON)
    validation_ok: Mapped[bool] = mapped_column(Boolean, default=False)
    validation_errors: Mapped[str | None] = mapped_column(Text, nullable=True)
    token_usage_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    report_date: Mapped[date] = mapped_column(Date, index=True)
    run_type: Mapped[str] = mapped_column(String(32), index=True)
    content: Mapped[str] = mapped_column(Text)
    success: Mapped[bool] = mapped_column(Boolean)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class RuleVersion(Base):
    __tablename__ = "rule_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    version: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    applied_from: Mapped[date] = mapped_column(Date, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RuleSuggestion(Base):
    __tablename__ = "rule_suggestions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    report_date: Mapped[date] = mapped_column(Date, index=True)
    code: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    suggestion_text: Mapped[str] = mapped_column(Text)
    source_llm_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), default="pending")
    reason_memo: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# FUND / INTEL / THEME / RAG

fund_state_enum = Enum("IN", "WATCH", "OUT", name="fund_state_enum", native_enum=False)
intel_session_enum = Enum("morning", "close", name="intel_session_enum", native_enum=False)
intel_queue_status_enum = Enum("pending", "done", "skipped", "failed", name="intel_queue_status_enum", native_enum=False)
approval_status_enum = Enum("draft", "approved", "rejected", name="approval_status_enum", native_enum=False)


class FundUniverseState(Base):
    __tablename__ = "fund_universe_state"

    code: Mapped[str] = mapped_column(String(16), primary_key=True)
    state: Mapped[str] = mapped_column(fund_state_enum, index=True)
    fund_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    risk_hard: Mapped[dict] = mapped_column(JSON, default=dict)
    risk_soft: Mapped[dict] = mapped_column(JSON, default=dict)
    tags: Mapped[dict] = mapped_column(JSON, default=dict)
    thesis_bull: Mapped[str | None] = mapped_column(Text, nullable=True)
    thesis_bear: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence_refs: Mapped[dict] = mapped_column(JSON, default=dict)
    data_gaps: Mapped[dict] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        index=True,
    )


class FundFeaturesSnapshot(Base):
    __tablename__ = "fund_features_snapshot"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(16), index=True)
    asof_date: Mapped[date] = mapped_column(Date, index=True)
    features: Mapped[dict] = mapped_column(JSON, default=dict)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (UniqueConstraint("code", "asof_date", name="uq_fund_features_snapshot"),)


class IntelQueue(Base):
    __tablename__ = "intel_queue"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    business_date: Mapped[date] = mapped_column(Date, index=True)
    session: Mapped[str] = mapped_column(intel_session_enum, index=True)
    code: Mapped[str] = mapped_column(String(16), index=True)
    priority: Mapped[float] = mapped_column(Float, default=0.0)
    sources_seed: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(intel_queue_status_enum, default="pending", index=True)
    idempotency_key: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (UniqueConstraint("business_date", "session", "code", name="uq_intel_queue_day_session_code"),)


class IntelItem(Base):
    __tablename__ = "intel_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(16), index=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    source_url: Mapped[str] = mapped_column(Text)
    source_type: Mapped[str] = mapped_column(String(64), index=True)
    headline: Mapped[str] = mapped_column(Text)
    summary: Mapped[str] = mapped_column(Text)
    facts: Mapped[dict] = mapped_column(JSON, default=dict)
    tags: Mapped[dict] = mapped_column(JSON, default=dict)
    risk_flags: Mapped[dict] = mapped_column(JSON, default=dict)
    critical_risk: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    evidence_refs: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class IntelDailyBudget(Base):
    __tablename__ = "intel_daily_budget"

    business_date: Mapped[date] = mapped_column(Date, primary_key=True)
    done_count: Mapped[int] = mapped_column(Integer, default=0)
    morning_done: Mapped[int] = mapped_column(Integer, default=0)
    close_done: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class Theme(Base):
    __tablename__ = "themes"

    theme_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    keywords: Mapped[dict] = mapped_column(JSON, default=dict)
    allowed_sources: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class ThemeStrengthDaily(Base):
    __tablename__ = "theme_strength_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    theme_id: Mapped[int] = mapped_column(Integer, index=True)
    asof_date: Mapped[date] = mapped_column(Date, index=True)
    strength: Mapped[float] = mapped_column(Float, default=0.0)
    drivers: Mapped[dict] = mapped_column(JSON, default=dict)

    __table_args__ = (UniqueConstraint("theme_id", "asof_date", name="uq_theme_strength_daily"),)


class ThemeSymbolMap(Base):
    __tablename__ = "theme_symbol_map"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    theme_id: Mapped[int] = mapped_column(Integer, index=True)
    code: Mapped[str] = mapped_column(String(16), index=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    rationale: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (UniqueConstraint("theme_id", "code", name="uq_theme_symbol_map"),)


class KbDocument(Base):
    __tablename__ = "kb_documents"

    doc_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    source_type: Mapped[str] = mapped_column(String(64), index=True)
    title: Mapped[str] = mapped_column(Text)
    tags: Mapped[dict] = mapped_column(JSON, default=dict)
    source_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    rights: Mapped[str | None] = mapped_column(String(64), nullable=True)
    sha256: Mapped[str] = mapped_column(String(64), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class KbChunk(Base):
    __tablename__ = "kb_chunks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_id: Mapped[str] = mapped_column(String(128), index=True)
    chunk_id: Mapped[int] = mapped_column(Integer)
    loc: Mapped[str] = mapped_column(String(128))
    text: Mapped[str] = mapped_column(Text)
    if Vector is None:
        embedding: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    else:  # pragma: no cover
        embedding: Mapped[list[float] | None] = mapped_column(Vector(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (UniqueConstraint("doc_id", "chunk_id", name="uq_kb_chunk"),)


class KbApproval(Base):
    __tablename__ = "kb_approvals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    item_id: Mapped[str] = mapped_column(String(128), index=True)
    item_type: Mapped[str] = mapped_column(String(32), index=True)
    status: Mapped[str] = mapped_column(approval_status_enum, default="draft", index=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (UniqueConstraint("item_id", "item_type", name="uq_kb_approval_item"),)


class FundRuleSuggestion(Base):
    __tablename__ = "fund_rule_suggestions"

    proposal_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scope: Mapped[str] = mapped_column(String(64), index=True)
    diff: Mapped[dict] = mapped_column(JSON, default=dict)
    why: Mapped[str] = mapped_column(Text)
    risk: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_effect: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class IntelRuleSuggestion(Base):
    __tablename__ = "intel_rule_suggestions"

    proposal_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scope: Mapped[str] = mapped_column(String(64), index=True)
    diff: Mapped[dict] = mapped_column(JSON, default=dict)
    why: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
