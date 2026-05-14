from shared_backend.clients.qdrant_client import (
    QdrantArticleEmbeddingPointRead,
    QdrantArticleEmbeddingPointSummaryRead,
    QdrantIndexingError,
    QdrantScoredArticleEmbeddingPointRead,
    SharedQdrantClient,
    build_article_embedding_point_id,
)


class SimpleQdrantClient(SharedQdrantClient):
    pass


__all__ = [
    "SimpleQdrantClient",
    "QdrantArticleEmbeddingPointRead",
    "QdrantArticleEmbeddingPointSummaryRead",
    "QdrantIndexingError",
    "QdrantScoredArticleEmbeddingPointRead",
    "build_article_embedding_point_id",
]
