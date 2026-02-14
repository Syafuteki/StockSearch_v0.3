from __future__ import annotations

import hashlib

from sqlalchemy import text
from sqlalchemy.orm import Session


def _lock_key(raw: str) -> int:
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return int(digest, 16) & 0x7FFFFFFFFFFFFFFF


def try_advisory_xact_lock(session: Session, lock_name: str) -> bool:
    key = _lock_key(lock_name)
    try:
        value = session.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": key}).scalar()
        return bool(value)
    except Exception:
        # Non-PostgreSQL backends might not support advisory locks; allow execution.
        return True

