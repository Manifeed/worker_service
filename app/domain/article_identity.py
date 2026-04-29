from __future__ import annotations

from datetime import datetime
import hashlib
import re
import unicodedata
from collections.abc import Sequence

from .source_identity import normalize_source_url


def normalize_article_identity_text(value: str | None) -> str | None:
    if value is None:
        return None

    normalized_value = unicodedata.normalize("NFKD", value)
    normalized_value = "".join(
        character
        for character in normalized_value
        if not unicodedata.combining(character)
    )
    normalized_value = normalized_value.casefold()
    normalized_value = re.sub(r"[^a-z0-9]+", " ", normalized_value)
    normalized_value = re.sub(r"\s+", " ", normalized_value).strip()
    return normalized_value or None


def build_article_content_key(
    *,
    title: str | None,
    summary: str | None,
    company: str | None,
    published_at: str | None,
) -> str | None:
    normalized_title = normalize_article_identity_text(title)
    normalized_summary = normalize_article_identity_text(summary)
    normalized_company = normalize_article_identity_text(company)
    normalized_published_on = _normalize_article_identity_date(published_at)
    if not all(
        (
            normalized_title,
            normalized_summary,
            normalized_company,
            normalized_published_on,
        )
    ):
        return None

    content_identity = "|".join(
        [
            "content",
            normalized_title,
            normalized_summary,
            normalized_company,
            normalized_published_on,
        ]
    )
    return hashlib.sha256(content_identity.encode("utf-8")).hexdigest()


def build_article_key(
    *,
    canonical_url: str | None,
    title: str | None,
    authors: Sequence[str] | None,
    company: str | None,
    published_at: str | None,
) -> str:
    normalized_url = normalize_source_url(canonical_url)
    if normalized_url:
        return hashlib.sha256(f"url|{normalized_url}".encode("utf-8")).hexdigest()

    normalized_title = normalize_article_identity_text(title) or ""
    normalized_author = _resolve_primary_author_identity(authors)
    normalized_company = normalize_article_identity_text(company) or ""
    normalized_published_at = (published_at or "").strip()
    fallback_identity = "|".join(
        [
            "fallback",
            normalized_title,
            normalized_author,
            normalized_company,
            normalized_published_at,
        ]
    )
    return hashlib.sha256(fallback_identity.encode("utf-8")).hexdigest()


def _resolve_primary_author_identity(authors: Sequence[str] | None) -> str:
    if not authors:
        return ""
    for author in authors:
        normalized_author = normalize_article_identity_text(author)
        if normalized_author:
            return normalized_author
    return ""


def _normalize_article_identity_date(value: str | None) -> str | None:
    raw_value = (value or "").strip()
    if not raw_value:
        return None
    if len(raw_value) >= 10 and raw_value[4] == "-" and raw_value[7] == "-":
        return raw_value[:10]
    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return None
