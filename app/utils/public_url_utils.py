from __future__ import annotations

from urllib.parse import urlsplit


def is_public_http_url(value: str | None, *, require_https: bool = False) -> bool:
    return normalize_public_http_url(value, require_https=require_https) is not None


def normalize_public_http_url(value: str | None, *, require_https: bool = False) -> str | None:
    raw_value = (value or "").strip()
    if not raw_value:
        return None
    if any(ord(character) < 32 or ord(character) == 127 for character in raw_value):
        return None

    try:
        parsed_url = urlsplit(raw_value)
        scheme = parsed_url.scheme.lower()
        hostname = parsed_url.hostname
        parsed_url.port
    except ValueError:
        return None

    if scheme not in {"http", "https"}:
        return None
    if require_https and scheme != "https":
        return None
    if not hostname:
        return None
    if parsed_url.username or parsed_url.password:
        return None

    return raw_value
