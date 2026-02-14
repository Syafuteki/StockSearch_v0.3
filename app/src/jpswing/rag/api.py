from __future__ import annotations

from typing import Any

from jpswing.db.session import DBSessionManager
from jpswing.rag.embedder import LocalEmbedder
from jpswing.rag.retrieval import retrieve as retrieve_chunks


class RagService:
    def __init__(self, *, db: DBSessionManager, embedder: LocalEmbedder) -> None:
        self.db = db
        self.embedder = embedder

    def retrieve(
        self,
        query: str,
        filters: dict[str, Any] | None = None,
        top_k: int = 5,
        *,
        for_llm: bool = True,
    ) -> list[dict[str, Any]]:
        with self.db.session_scope() as session:
            return retrieve_chunks(
                session,
                embedder=self.embedder,
                query=query,
                filters=filters,
                top_k=top_k,
                for_llm=for_llm,
            )

