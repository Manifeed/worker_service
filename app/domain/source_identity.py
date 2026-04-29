from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from app.utils.public_url_utils import normalize_public_http_url


_TRACKING_QUERY_PARAM_NAMES = frozenset(
    {
        "_ga",
        "_gl",
        "fbclid",
        "gclid",
        "mc_cid",
        "mc_eid",
        "xtor",
    }
)


def normalize_source_url(url: str | None) -> str:
    raw_url = normalize_public_http_url(url)
    if not raw_url:
        return ""

    parsed_url = urlsplit(raw_url)
    scheme = parsed_url.scheme.lower()
    hostname = (parsed_url.hostname or "").lower()
    port = parsed_url.port

    netloc = hostname
    if parsed_url.username:
        netloc = f"{parsed_url.username}@{netloc}"
    if port is not None and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{netloc}:{port}"

    path = parsed_url.path or ""
    if path not in {"", "/"}:
        path = path.rstrip("/")

    filtered_query_params: list[tuple[str, str]] = []
    for query_key, query_value in parse_qsl(parsed_url.query, keep_blank_values=True):
        normalized_key = query_key.strip()
        if not normalized_key:
            continue
        normalized_key_lower = normalized_key.lower()
        if normalized_key_lower.startswith("utm_"):
            continue
        if normalized_key_lower.startswith("at_"):
            continue
        if normalized_key_lower in _TRACKING_QUERY_PARAM_NAMES:
            continue
        filtered_query_params.append((normalized_key, query_value))

    filtered_query_params.sort(key=lambda item: (item[0].lower(), item[1], item[0]))
    normalized_query = urlencode(filtered_query_params, doseq=True)
    normalized_url = urlunsplit((scheme, netloc, path, normalized_query, ""))

    if normalized_url.endswith("?#"):
        return normalized_url[:-2]
    if normalized_url.endswith("?"):
        return normalized_url[:-1]
    return normalized_url
