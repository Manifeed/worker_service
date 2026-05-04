from __future__ import annotations

import os

DEFAULT_RSS_WORKER_VERSION = "0.1.4"


def resolve_default_rss_worker_version() -> str:
    worker_version = os.getenv(
        "RSS_WORKER_VERSION",
        DEFAULT_RSS_WORKER_VERSION,
    ).strip()
    if not worker_version:
        return DEFAULT_RSS_WORKER_VERSION
    return worker_version
