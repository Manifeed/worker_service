from __future__ import annotations

import os

from shared_backend.domain.source_embedding_config import (
    FIXED_SOURCE_EMBEDDING_MODEL_NAME,
    resolve_qdrant_api_key,
    resolve_qdrant_collection_name,
    resolve_qdrant_url,
    resolve_source_embedding_dimensions,
    resolve_source_embedding_model_name,
)

DEFAULT_EMBEDDING_REDIS_QUEUE = "embedding:source_embedding"
DEFAULT_SOURCE_EMBEDDING_BATCH_SIZE = 128


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


def resolve_embed_task_lease_seconds() -> int:
    raw_value = os.getenv("EMBED_TASK_LEASE_SECONDS", "").strip()
    if raw_value:
        try:
            parsed = int(raw_value)
        except ValueError:
            parsed = 900
        if parsed >= 30:
            return parsed
    return 900
