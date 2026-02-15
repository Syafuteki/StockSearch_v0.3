from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Any
import logging

from sqlalchemy import delete, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

from .models import Base


class DBSessionManager:
    def __init__(self, database_url: str, echo: bool = False) -> None:
        self.engine: Engine = create_engine(database_url, echo=echo, future=True)
        self._session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, future=True)
        self.logger = logging.getLogger(self.__class__.__name__)

    def init_schema(self) -> None:
        # Ensure pgvector extension exists before creating VECTOR columns.
        if self.engine.dialect.name == "postgresql":
            try:
                with self.engine.begin() as conn:
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Failed to enable pgvector extension: %s", exc)
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session_scope(self) -> Session:
        session: Session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def replace_rows_for_date(
    session: Session,
    model: Any,
    target_date: date,
    *,
    date_field: str = "trade_date",
    extra_filters: dict[str, Any] | None = None,
) -> None:
    model_date_col = getattr(model, date_field)
    stmt = delete(model).where(model_date_col == target_date)
    for key, value in (extra_filters or {}).items():
        stmt = stmt.where(getattr(model, key) == value)
    session.execute(stmt)


def get_latest_shortlist_codes_before(session: Session, model: Any, target_date: date, rule_version: str) -> set[str]:
    date_col = getattr(model, "trade_date")
    rule_col = getattr(model, "rule_version")
    code_col = getattr(model, "code")
    latest_date = session.scalar(
        select(date_col).where(date_col < target_date, rule_col == rule_version).order_by(date_col.desc()).limit(1)
    )
    if latest_date is None:
        return set()
    rows = session.execute(select(code_col).where(date_col == latest_date, rule_col == rule_version)).all()
    return {row[0] for row in rows}
