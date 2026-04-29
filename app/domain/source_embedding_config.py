from __future__ import annotations

import os

FIXED_SOURCE_EMBEDDING_MODEL_NAME = "Xenova/multilingual-e5-large"
DEFAULT_SOURCE_EMBEDDING_WORKER_VERSION = "e5-large-v1"
DEFAULT_QDRANT_URL = "http://qdrant:6333"
DEFAULT_QDRANT_COLLECTION = "article_embeddings"


def resolve_default_source_embedding_worker_version() -> str:
    worker_version = os.getenv(
        "SOURCE_EMBEDDING_WORKER_VERSION",
        DEFAULT_SOURCE_EMBEDDING_WORKER_VERSION,
    ).strip()
    if not worker_version:
        return DEFAULT_SOURCE_EMBEDDING_WORKER_VERSION
    return worker_version


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
