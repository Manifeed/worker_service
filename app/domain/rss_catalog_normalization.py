from __future__ import annotations

from urllib.parse import urlsplit


def normalize_host(host: str | None) -> str | None:
    if host is None:
        return None

    normalized_host = host.strip()
    if not normalized_host:
        return None

    prefixed_host = normalized_host if "://" in normalized_host else f"//{normalized_host}"
    parsed_host = urlsplit(prefixed_host)
    hostname = parsed_host.hostname
    if hostname is None:
        return None

    normalized_host = hostname.strip().lower()
    return normalized_host or None
