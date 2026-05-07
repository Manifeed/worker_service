from __future__ import annotations

import re
import unicodedata


_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")


def build_worker_name(*, pseudo: str, worker_type: str, worker_number: int) -> str:
    normalized_pseudo = normalize_user_pseudo(pseudo) or "worker"
    normalized_worker_type = _worker_type_slug(worker_type)
    normalized_worker_number = max(1, int(worker_number))
    return f"{normalized_pseudo}-{normalized_worker_type}-{normalized_worker_number}"


def _worker_type_slug(worker_type: str) -> str:
    if worker_type == "rss_scrapper":
        return "rss"
    return normalize_user_pseudo(worker_type) or "worker"


def normalize_user_pseudo(value: str) -> str:
    ascii_value = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    return _NON_ALNUM_PATTERN.sub("-", ascii_value).strip("-")
