from __future__ import annotations

import math
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from jpswing.db.models import KbChunk, KbDocument
from jpswing.rag.embedder import LocalEmbedder


def _dot(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    return sum(a[i] * b[i] for i in range(n))


def _norm(v: list[float]) -> float:
    return math.sqrt(sum(x * x for x in v))


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    na = _norm(a)
    nb = _norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return _dot(a, b) / (na * nb)


def retrieve(
    session: Session,
    *,
    embedder: LocalEmbedder,
    query: str,
    filters: dict[str, Any] | None = None,
    top_k: int = 5,
    for_llm: bool = True,
) -> list[dict[str, Any]]:
    filters = filters or {}
    stmt = select(KbChunk, KbDocument).join(KbDocument, KbDocument.doc_id == KbChunk.doc_id)
    source_type = filters.get("source_type")
    if source_type:
        stmt = stmt.where(KbDocument.source_type == source_type)
    if for_llm:
        stmt = stmt.where(KbDocument.source_type != "books_fulltext")
    rows = session.execute(stmt).all()
    if not rows:
        return []

    query_vecs = embedder.embed([query])
    query_vec = query_vecs[0] if query_vecs else []
    ranked: list[dict[str, Any]] = []
    for chunk, doc in rows:
        emb = chunk.embedding if isinstance(chunk.embedding, list) else []
        score = _cosine(query_vec, emb)
        ranked.append(
            {
                "doc_id": doc.doc_id,
                "title": doc.title,
                "source_type": doc.source_type,
                "chunk_id": chunk.chunk_id,
                "loc": chunk.loc,
                "text": chunk.text,
                "score": score,
            }
        )
    ranked.sort(key=lambda x: (-x["score"], x["doc_id"], x["chunk_id"]))
    return ranked[:top_k]

