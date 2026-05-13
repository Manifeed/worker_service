from __future__ import annotations

import os

FIXED_SOURCE_EMBEDDING_MODEL_NAME = "BAAI/bge-m3"
DEFAULT_EMBEDDING_REDIS_QUEUE = "embedding:source_embedding"
DEFAULT_SOURCE_EMBEDDING_BATCH_SIZE = 128
DEFAULT_QDRANT_URL = "http://qdrant:6333"
DEFAULT_QDRANT_COLLECTION = "article_embeddings"


def resolve_source_embedding_model_name() -> str:
    return FIXED_SOURCE_EMBEDDING_MODEL_NAME


def resolve_embedding_redis_queue_name() -> str:
    return os.getenv("EMBEDDING_REDIS_QUEUE", DEFAULT_EMBEDDING_REDIS_QUEUE).strip() or DEFAULT_EMBEDDING_REDIS_QUEUE


def resolve_source_embedding_batch_size() -> int:
    raw_value = os.getenv("SOURCE_EMBEDDING_BATCH_SIZE", "").strip()
    if raw_value:
        try:
            parsed = int(raw_value)
        except ValueError:
            parsed = DEFAULT_SOURCE_EMBEDDING_BATCH_SIZE
        if parsed > 0:
            return parsed
    return DEFAULT_SOURCE_EMBEDDING_BATCH_SIZE


def resolve_source_embedding_dimensions() -> int | None:
    raw_value = os.getenv("SOURCE_EMBEDDING_DIMENSIONS", "").strip()
    if not raw_value:
        return None
    try:
        parsed = int(raw_value)
    except ValueError:
        return None
    if parsed <= 0:
        return None
    return parsed


def resolve_qdrant_url() -> str:
    qdrant_url = os.getenv("QDRANT_URL", DEFAULT_QDRANT_URL).strip()
    if not qdrant_url:
        return DEFAULT_QDRANT_URL
    return qdrant_url.rstrip("/")


def resolve_qdrant_collection_name() -> str:
    collection_name = os.getenv(
        "QDRANT_COLLECTION_NAME",
        DEFAULT_QDRANT_COLLECTION,
    ).strip()
    if not collection_name:
        return DEFAULT_QDRANT_COLLECTION
    return collection_name


def resolve_qdrant_api_key() -> str | None:
    api_key = os.getenv("QDRANT_API_KEY", "").strip()
    if not api_key:
        return None
    return api_key
