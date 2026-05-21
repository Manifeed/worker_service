from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.domain.article_authors import coerce_article_author_names
from app.domain.language_detection import detect_article_language
from app.schemas.workers.worker_result_schema import WorkerResultSchema
from shared_backend.domain.article_identity import build_article_content_key, build_article_key
from shared_backend.domain.source_identity import normalize_source_url
from shared_backend.utils.datetime_utils import normalize_datetime_to_utc
from shared_backend.utils.public_url import normalize_public_http_url


ARTICLE_IMAGE_URL_MAX_LENGTH = 1000
NORMALIZED_ARTICLE_URL_MAX_LENGTH = 1000


@dataclass(frozen=True)
class FeedContext:
    feed_id: int
    feed_url: str | None
    company_id: int | None
    company_name: str | None
    country: str


@dataclass(frozen=True)
class CandidateRow:
    feed_id: int
    article_key: str
    content_key: str | None
    published_at: datetime | None
    canonical_url: str
    source_urls: tuple[str, ...]
    title: str
    summary: str | None
    authors: tuple[str, ...]
    image_url: str | None
    company_id: int | None
    country: str
    language: str
    duplicate_hint: bool


def load_feed_contexts(
    db: Session,
    *,
    feed_ids: list[int],
) -> dict[int, FeedContext]:
    normalized_feed_ids = sorted({int(feed_id) for feed_id in feed_ids})
    if not normalized_feed_ids:
        return {}
    rows = (
        db.execute(
            text(
                """
                SELECT
                    feed.id AS feed_id,
                    feed.url AS feed_url,
                    feed.company_id,
                    company.name AS company_name,
                    COALESCE(NULLIF(company.country, ''), 'xx') AS country
                FROM rss_feeds AS feed
                LEFT JOIN rss_company AS company
                    ON company.id = feed.company_id
                WHERE feed.id = ANY(:feed_ids)
                """
            ),
            {"feed_ids": normalized_feed_ids},
        )
        .mappings()
        .all()
    )
    return {
        int(row["feed_id"]): FeedContext(
            feed_id=int(row["feed_id"]),
            feed_url=str(row["feed_url"]) if row["feed_url"] is not None else None,
            company_id=int(row["company_id"]) if row["company_id"] is not None else None,
            company_name=str(row["company_name"]) if row["company_name"] is not None else None,
            country=str(row["country"] or "xx"),
        )
        for row in rows
    }


def build_candidate_rows(
    db: Session,
    *,
    feed_contexts: dict[int, FeedContext],
    results: list[WorkerResultSchema],
) -> list[CandidateRow]:
    provisional_rows: list[CandidateRow] = []
    article_keys: list[str] = []
    content_keys: list[str] = []
    for result in results:
        feed_context = feed_contexts.get(result.feed_id)
        for source in result.sources:
            author_names = coerce_article_author_names(
                author_names=source.authors,
                author=source.author,
            )
            published_at_text = None
            if source.published_at is not None:
                published_at_text = normalize_datetime_to_utc(source.published_at).isoformat()
            canonical_url = resolve_primary_canonical_url(source.urls)
            if not canonical_url:
                continue
            article_key = build_article_key(
                canonical_url=canonical_url,
                title=source.title,
                authors=author_names,
                company=feed_context.company_name if feed_context is not None else None,
                published_at=published_at_text,
            )
            content_key = build_article_content_key(
                title=source.title,
                summary=source.summary,
                company=feed_context.company_name if feed_context is not None else None,
                published_at=published_at_text,
            )
            provisional_rows.append(
                CandidateRow(
                    feed_id=result.feed_id,
                    article_key=article_key,
                    content_key=content_key,
                    published_at=source.published_at,
                    canonical_url=canonical_url,
                    source_urls=tuple(source.urls),
                    title=source.title,
                    summary=source.summary,
                    authors=tuple(author_names),
                    image_url=normalize_article_image_url(source.image_url),
                    company_id=feed_context.company_id if feed_context is not None else None,
                    country=feed_context.country if feed_context is not None else "xx",
                    language=detect_article_language(
                        country=feed_context.country if feed_context is not None else "xx",
                        title=source.title,
                        summary=source.summary,
                        urls=source.urls + ([feed_context.feed_url] if feed_context and feed_context.feed_url else []),
                    ),
                    duplicate_hint=False,
                )
            )
            article_keys.append(article_key)
            if content_key:
                content_keys.append(content_key)

    existing_article_keys = _load_existing_article_keys(db, article_keys=article_keys)
    existing_content_keys = _load_existing_content_keys(db, content_keys=content_keys)
    seen_article_keys: set[str] = set()
    seen_content_keys: set[str] = set()
    candidate_rows: list[CandidateRow] = []
    for provisional_row in provisional_rows:
        article_key = provisional_row.article_key
        content_key = provisional_row.content_key
        duplicate_hint = article_key in seen_article_keys or article_key in existing_article_keys
        if content_key:
            duplicate_hint = (
                duplicate_hint
                or content_key in seen_content_keys
                or content_key in existing_content_keys
            )
        seen_article_keys.add(article_key)
        if content_key:
            seen_content_keys.add(content_key)
        candidate_rows.append(
            CandidateRow(
                feed_id=provisional_row.feed_id,
                article_key=article_key,
                content_key=content_key,
                published_at=provisional_row.published_at,
                canonical_url=provisional_row.canonical_url,
                source_urls=provisional_row.source_urls,
                title=provisional_row.title,
                summary=provisional_row.summary,
                authors=provisional_row.authors,
                image_url=provisional_row.image_url,
                company_id=provisional_row.company_id,
                country=provisional_row.country,
                language=provisional_row.language,
                duplicate_hint=duplicate_hint,
            )
        )
    return candidate_rows


def normalize_article_image_url(value: str | None) -> str | None:
    normalized_value = normalize_public_http_url(value, require_https=True)
    if normalized_value is None:
        return None
    if len(normalized_value) > ARTICLE_IMAGE_URL_MAX_LENGTH:
        return None
    return normalized_value


def resolve_primary_canonical_url(raw_urls: list[str]) -> str | None:
    for raw_url in raw_urls:
        canonical_url = normalize_source_url(raw_url)
        if canonical_url:
            return canonical_url
    return None


def upsert_article_url_variants(
    db: Session,
    *,
    article_id: int,
    raw_urls: tuple[str, ...],
) -> None:
    seen_normalized_urls: set[str] = set()
    for raw_url in raw_urls:
        normalized_url = normalize_source_url(raw_url)
        if not normalized_url or normalized_url in seen_normalized_urls:
            continue
        if len(normalized_url) > NORMALIZED_ARTICLE_URL_MAX_LENGTH:
            continue
        seen_normalized_urls.add(normalized_url)
        db.execute(
            text(
                """
                INSERT INTO article_url (
                    article_id,
                    url
                ) VALUES (
                    :article_id,
                    :url
                )
                ON CONFLICT (url) DO UPDATE SET
                    article_id = EXCLUDED.article_id
                """
            ),
            {
                "article_id": article_id,
                "url": normalized_url,
            },
        )


def _load_existing_article_keys(db: Session, *, article_keys: list[str]) -> set[str]:
    normalized_article_keys = sorted({article_key for article_key in article_keys if article_key})
    if not normalized_article_keys:
        return set()
    rows = db.execute(
        text(
            """
            SELECT article_key
            FROM articles
            WHERE article_key = ANY(:article_keys)
            """
        ),
        {"article_keys": normalized_article_keys},
    ).scalars().all()
    return {str(article_key) for article_key in rows}


def _load_existing_content_keys(db: Session, *, content_keys: list[str]) -> set[str]:
    normalized_content_keys = sorted({content_key for content_key in content_keys if content_key})
    if not normalized_content_keys:
        return set()
    rows = db.execute(
        text(
            """
            SELECT content_key
            FROM articles
            WHERE content_key = ANY(:content_keys)
            """
        ),
        {"content_keys": normalized_content_keys},
    ).scalars().all()
    return {str(content_key) for content_key in rows if content_key is not None}
