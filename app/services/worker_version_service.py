from __future__ import annotations

from app.domain.rss_worker_config import resolve_default_rss_worker_version
from app.services.worker_release_service import resolve_active_rss_worker_version


def resolve_rss_worker_version() -> str:
    return resolve_active_rss_worker_version() or resolve_default_rss_worker_version()


def resolve_source_embedding_worker_version() -> str:
    raise RuntimeError("source_embedding is no longer resolved as a desktop worker version")
