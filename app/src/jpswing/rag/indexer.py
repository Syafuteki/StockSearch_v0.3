from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from jpswing.db.models import IntelItem, KbApproval, KbChunk, KbDocument
from jpswing.rag.embedder import LocalEmbedder


def _split_front_matter(text: str) -> tuple[dict[str, Any], str]:
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---\n", 4)
    if end == -1:
        return {}, text
    meta_raw = text[4:end]
    body = text[end + 5 :]
    meta: dict[str, Any] = {}
    for line in meta_raw.splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        meta[k.strip()] = v.strip()
    return meta, body


def _chunk_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    if not text:
        return []
    chunks: list[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(n, start + chunk_size)
        chunks.append(text[start:end].strip())
        if end >= n:
            break
        start = max(0, end - overlap)
    return [c for c in chunks if c]


class KbIndexer:
    def __init__(self, *, embedder: LocalEmbedder, chunk_size: int = 700, chunk_overlap: int = 120) -> None:
        self.embedder = embedder
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.logger = logging.getLogger(self.__class__.__name__)

    def index_markdown_dir(self, session: Session, kb_dir: str | Path = "kb") -> int:
        base = Path(kb_dir)
        if not base.exists():
            self.logger.info("kb directory not found: %s", base)
            return 0
        count = 0
        for path in sorted(base.glob("*.md")):
            text = path.read_text(encoding="utf-8")
            meta, body = _split_front_matter(text)
            title = str(meta.get("title") or path.stem)
            source_type = str(meta.get("source_type") or "human_markdown")
            tags = [x.strip() for x in str(meta.get("tags") or "").split(",") if x.strip()]
            self._upsert_document(
                session=session,
                doc_id=str(path.relative_to(base)),
                source_type=source_type,
                title=title,
                tags=tags,
                source_id=str(path),
                rights=str(meta.get("rights") or "internal"),
                body=body,
            )
            count += 1
        return count

    def promote_approved_items(self, session: Session) -> int:
        approvals = session.execute(
            select(KbApproval).where(KbApproval.status == "approved", KbApproval.item_type == "intel_item")
        ).scalars().all()
        count = 0
        for approval in approvals:
            if not approval.item_id.isdigit():
                continue
            intel = session.get(IntelItem, int(approval.item_id))
            if intel is None:
                continue
            body = f"{intel.headline}\n\n{intel.summary}\n\nFacts: {intel.facts}"
            self._upsert_document(
                session=session,
                doc_id=f"intel:{intel.id}",
                source_type="system_intel",
                title=intel.headline,
                tags=list((intel.tags or {}).get("items", []) if isinstance(intel.tags, dict) else []),
                source_id=str(intel.id),
                rights="internal",
                body=body,
            )
            count += 1
        return count

    def _upsert_document(
        self,
        *,
        session: Session,
        doc_id: str,
        source_type: str,
        title: str,
        tags: list[str],
        source_id: str,
        rights: str,
        body: str,
    ) -> None:
        sha = hashlib.sha256(body.encode("utf-8")).hexdigest()
        existing = session.get(KbDocument, doc_id)
        if existing is not None and existing.sha256 == sha:
            return
        if existing is None:
            existing = KbDocument(
                doc_id=doc_id,
                source_type=source_type,
                title=title,
                tags={"items": tags},
                source_id=source_id,
                rights=rights,
                sha256=sha,
            )
            session.add(existing)
        else:
            existing.source_type = source_type
            existing.title = title
            existing.tags = {"items": tags}
            existing.source_id = source_id
            existing.rights = rights
            existing.sha256 = sha

        session.query(KbChunk).filter(KbChunk.doc_id == doc_id).delete()
        chunks = _chunk_text(body, self.chunk_size, self.chunk_overlap)
        vectors = self.embedder.embed(chunks) if chunks else []
        for idx, chunk in enumerate(chunks):
            vec = vectors[idx] if idx < len(vectors) else []
            session.add(
                KbChunk(
                    doc_id=doc_id,
                    chunk_id=idx,
                    loc=f"chunk:{idx}",
                    text=chunk,
                    embedding=vec,
                )
            )

