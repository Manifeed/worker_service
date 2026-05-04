from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.domain.article_authors import (
    coerce_article_author_names,
    normalize_article_author_name,
)
from app.domain.article_identity import build_article_content_key, build_article_key
from app.domain.source_identity import normalize_source_url
from app.schemas.workers.worker_result_schema import WorkerResultSchema
from app.schemas.workers.worker_rss_result_schema import WorkerRssTaskLocalDedupSchema
from shared_backend.utils.datetime_utils import normalize_datetime_to_utc
from shared_backend.utils.public_url import normalize_public_http_url


@dataclass(frozen=True)
class _FeedContext:
    feed_id: int
    company_id: int | None
    company_name: str | None


@dataclass(frozen=True)
class _CandidateRow:
    feed_id: int
    article_key: str
    content_key: str | None
    published_at: datetime | None
    canonical_url: str
    title: str
    summary: str | None
    authors: tuple[str, ...]
    image_url: str | None
    company_id: int | None
    duplicate_hint: bool


def persist_rss_task_results(
    db: Session,
    *,
    trace_id: str,
    lease_id: str,
    worker_name: str,
    local_dedup: WorkerRssTaskLocalDedupSchema,
    results: list[WorkerResultSchema],
) -> None:
    del trace_id, lease_id, worker_name, local_dedup
    feed_contexts = _load_feed_contexts(
        db,
        feed_ids=[result.feed_id for result in results],
    )
    candidate_rows = _build_candidate_rows(
        db,
        feed_contexts=feed_contexts,
        results=results,
    )
    _merge_candidates_into_articles(db, candidate_rows=candidate_rows)
    _upsert_rss_feed_runtime(
        db,
        results=results,
    )


def _load_feed_contexts(
    db: Session,
    *,
    feed_ids: list[int],
) -> dict[int, _FeedContext]:
    normalized_feed_ids = sorted({int(feed_id) for feed_id in feed_ids})
    if not normalized_feed_ids:
        return {}

    rows = (
        db.execute(
            text(
                """
                SELECT
                    feed.id AS feed_id,
                    feed.company_id,
                    company.name AS company_name
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
        int(row["feed_id"]): _FeedContext(
            feed_id=int(row["feed_id"]),
            company_id=(int(row["company_id"]) if row["company_id"] is not None else None),
            company_name=(str(row["company_name"]) if row["company_name"] is not None else None),
        )
        for row in rows
    }


def _build_candidate_rows(
    db: Session,
    *,
    feed_contexts: dict[int, _FeedContext],
    results: list[WorkerResultSchema],
) -> list[_CandidateRow]:
    provisional_rows: list[_CandidateRow] = []
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
            canonical_url = normalize_source_url(source.url)
            if not canonical_url:
                continue
            article_key = build_article_key(
                canonical_url=canonical_url,
                title=source.title,
                authors=author_names,
                company=(feed_context.company_name if feed_context is not None else None),
                published_at=published_at_text,
            )
            content_key = build_article_content_key(
                title=source.title,
                summary=source.summary,
                company=(feed_context.company_name if feed_context is not None else None),
                published_at=published_at_text,
            )
            provisional_rows.append(
                _CandidateRow(
                    feed_id=result.feed_id,
                    article_key=article_key,
                    content_key=content_key,
                    published_at=source.published_at,
                    canonical_url=canonical_url,
                    title=source.title,
                    summary=source.summary,
                    authors=tuple(author_names),
                    image_url=normalize_public_http_url(source.image_url, require_https=True),
                    company_id=(feed_context.company_id if feed_context is not None else None),
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
    candidate_rows: list[_CandidateRow] = []
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
            _CandidateRow(
                feed_id=provisional_row.feed_id,
                article_key=article_key,
                content_key=content_key,
                published_at=provisional_row.published_at,
                canonical_url=provisional_row.canonical_url,
                title=provisional_row.title,
                summary=provisional_row.summary,
                authors=provisional_row.authors,
                image_url=provisional_row.image_url,
                company_id=provisional_row.company_id,
                duplicate_hint=duplicate_hint,
            )
        )
    return candidate_rows


def _load_existing_article_keys(
    db: Session,
    *,
    article_keys: list[str],
) -> set[str]:
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


def _load_existing_content_keys(
    db: Session,
    *,
    content_keys: list[str],
) -> set[str]:
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


def _merge_candidates_into_articles(
    db: Session,
    *,
    candidate_rows: list[_CandidateRow],
) -> None:
    article_ids_by_key: dict[str, int] = {}
    article_ids_by_content_key: dict[str, int] = {}
    for candidate_row in candidate_rows:
        article_id = article_ids_by_key.get(candidate_row.article_key)
        if article_id is None and candidate_row.content_key:
            article_id = article_ids_by_content_key.get(candidate_row.content_key)
        if article_id is None:
            article_id, resolved_article_key = _upsert_article(db, candidate_row=candidate_row)
            article_ids_by_key[resolved_article_key] = article_id
        article_ids_by_key[candidate_row.article_key] = article_id
        if candidate_row.content_key:
            article_ids_by_content_key[candidate_row.content_key] = article_id
        _sync_article_authors(
            db,
            article_id=article_id,
            author_names=candidate_row.authors,
        )
        _upsert_article_feed_link(
            db,
            article_id=article_id,
            feed_id=candidate_row.feed_id,
        )


def _upsert_article(
    db: Session,
    *,
    candidate_row: _CandidateRow,
) -> tuple[int, str]:
    existing_article = _find_existing_article(db, candidate_row=candidate_row)
    if existing_article is not None:
        existing_article_id, existing_article_key = existing_article
        db.execute(
            text(
                """
                UPDATE articles
                SET
                    published_at = COALESCE(articles.published_at, :published_at),
                    canonical_url = COALESCE(NULLIF(:canonical_url, ''), articles.canonical_url),
                    title = COALESCE(NULLIF(:title, ''), articles.title),
                    summary = COALESCE(:summary, articles.summary),
                    image_url = COALESCE(:image_url, articles.image_url),
                    content_key = COALESCE(articles.content_key, :content_key),
                    company_id = COALESCE(:company_id, articles.company_id)
                WHERE article_id = :article_id
                """
            ),
            {
                "article_id": existing_article_id,
                "published_at": candidate_row.published_at,
                "canonical_url": candidate_row.canonical_url,
                "title": candidate_row.title,
                "summary": candidate_row.summary,
                "image_url": candidate_row.image_url,
                "content_key": candidate_row.content_key,
                "company_id": candidate_row.company_id,
            },
        )
        return existing_article_id, existing_article_key

    article_id: int | None = None
    try:
        with db.begin_nested():
            article_id = db.execute(
                text(
                    """
                    INSERT INTO articles (
                        article_key,
                        content_key,
                        published_at,
                        ingested_at,
                        canonical_url,
                        title,
                        summary,
                        image_url,
                        language,
                        company_id
                    ) VALUES (
                        :article_key,
                        :content_key,
                        :published_at,
                        now(),
                        :canonical_url,
                        :title,
                        :summary,
                        :image_url,
                        NULL,
                        :company_id
                    )
                    RETURNING article_id
                    """
                ),
                {
                    "article_key": candidate_row.article_key,
                    "content_key": candidate_row.content_key,
                    "published_at": candidate_row.published_at,
                    "canonical_url": candidate_row.canonical_url,
                    "title": candidate_row.title,
                    "summary": candidate_row.summary,
                    "image_url": candidate_row.image_url,
                    "company_id": candidate_row.company_id,
                },
            ).scalar_one()
    except IntegrityError:
        existing_article = _find_existing_article(db, candidate_row=candidate_row)
        if existing_article is None:
            raise
        existing_article_id, existing_article_key = existing_article
        db.execute(
            text(
                """
                UPDATE articles
                SET
                    published_at = COALESCE(articles.published_at, :published_at),
                    canonical_url = COALESCE(NULLIF(:canonical_url, ''), articles.canonical_url),
                    title = COALESCE(NULLIF(:title, ''), articles.title),
                    summary = COALESCE(:summary, articles.summary),
                    image_url = COALESCE(:image_url, articles.image_url),
                    content_key = COALESCE(articles.content_key, :content_key),
                    company_id = COALESCE(:company_id, articles.company_id)
                WHERE article_id = :article_id
                """
            ),
            {
                "article_id": existing_article_id,
                "published_at": candidate_row.published_at,
                "canonical_url": candidate_row.canonical_url,
                "title": candidate_row.title,
                "summary": candidate_row.summary,
                "image_url": candidate_row.image_url,
                "content_key": candidate_row.content_key,
                "company_id": candidate_row.company_id,
            },
        )
        return existing_article_id, existing_article_key

    if article_id is None:
        raise RuntimeError("Expected inserted article_id after article upsert")
    return int(article_id), candidate_row.article_key


def _find_existing_article(
    db: Session,
    *,
    candidate_row: _CandidateRow,
) -> tuple[int, str] | None:
    existing_article_row = (
        db.execute(
            text(
                """
                SELECT article_id, article_key
                FROM articles
                WHERE article_key = :article_key
                """
            ),
            {"article_key": candidate_row.article_key},
        )
        .mappings()
        .first()
    )
    if existing_article_row is not None:
        return int(existing_article_row["article_id"]), str(existing_article_row["article_key"])

    if not candidate_row.content_key:
        return None

    existing_article_row = (
        db.execute(
            text(
                """
                SELECT article_id, article_key
                FROM articles
                WHERE content_key = :content_key
                ORDER BY published_at DESC NULLS LAST, article_id DESC
                LIMIT 1
                """
            ),
            {"content_key": candidate_row.content_key},
        )
        .mappings()
        .first()
    )
    if existing_article_row is None:
        return None
    return int(existing_article_row["article_id"]), str(existing_article_row["article_key"])


def _sync_article_authors(
    db: Session,
    *,
    article_id: int,
    author_names: tuple[str, ...],
) -> None:
    if not author_names:
        return

    existing_author_names = tuple(
        str(row["display_name"])
        for row in db.execute(
            text(
                """
                SELECT author.display_name
                FROM article_authors AS article_author
                JOIN authors AS author
                    ON author.id = article_author.author_id
                WHERE article_author.article_id = :article_id
                ORDER BY article_author.position ASC
                """
            ),
            {"article_id": article_id},
        ).mappings()
    )
    existing_normalized_names = tuple(
        normalized_name
        for author_name in existing_author_names
        if (normalized_name := normalize_article_author_name(author_name)) is not None
    )
    desired_normalized_names = tuple(
        normalized_name
        for author_name in author_names
        if (normalized_name := normalize_article_author_name(author_name)) is not None
    )
    if existing_normalized_names == desired_normalized_names:
        return

    db.execute(
        text(
            """
            DELETE FROM article_authors
            WHERE article_id = :article_id
            """
        ),
        {"article_id": article_id},
    )
    for position, author_name in enumerate(author_names, start=1):
        author_id = _upsert_author(db, author_name=author_name)
        db.execute(
            text(
                """
                INSERT INTO article_authors (
                    article_id,
                    author_id,
                    position
                ) VALUES (
                    :article_id,
                    :author_id,
                    :position
                )
                """
            ),
            {
                "article_id": article_id,
                "author_id": author_id,
                "position": position,
            },
        )


def _upsert_author(db: Session, *, author_name: str) -> int:
    normalized_author_name = normalize_article_author_name(author_name)
    if normalized_author_name is None:
        raise ValueError("author_name must normalize to a non-empty value")

    author_id = db.execute(
        text(
            """
            INSERT INTO authors (
                normalized_name,
                display_name
            ) VALUES (
                :normalized_name,
                :display_name
            )
            ON CONFLICT (normalized_name) DO UPDATE SET
                display_name = authors.display_name
            RETURNING id
            """
        ),
        {
            "normalized_name": normalized_author_name,
            "display_name": author_name,
        },
    ).scalar_one()
    return int(author_id)


def _upsert_article_feed_link(
    db: Session,
    *,
    article_id: int,
    feed_id: int,
) -> None:
    db.execute(
        text(
            """
            INSERT INTO article_feed_links (
                article_id,
                feed_id,
                first_seen_at
            ) VALUES (
                :article_id,
                :feed_id,
                now()
            )
            ON CONFLICT (article_id, feed_id) DO NOTHING
            """
        ),
        {
            "article_id": article_id,
            "feed_id": feed_id,
        },
    )


def _upsert_rss_feed_runtime(
    db: Session,
    *,
    results: list[WorkerResultSchema],
) -> None:
    for result in results:
        db.execute(
            text(
                """
                INSERT INTO rss_feed_runtime (
                    feed_id,
                    last_scraped_at,
                    last_status,
                    etag,
                    last_feed_update,
                    last_article_published_at,
                    consecutive_error_count,
                    last_error_at,
                    last_error_code
                ) VALUES (
                    :feed_id,
                    now(),
                    CAST(:status AS rss_feed_runtime_status),
                    :etag,
                    :last_feed_update,
                    :last_article_published_at,
                    CASE
                        WHEN :status = 'error' THEN 1
                        ELSE 0
                    END,
                    CASE
                        WHEN :status = 'error' THEN now()
                        ELSE NULL
                    END,
                    CASE
                        WHEN :status = 'error' THEN :error_code
                        ELSE NULL
                    END
                )
                ON CONFLICT (feed_id) DO UPDATE SET
                    last_scraped_at = now(),
                    last_status = CAST(:status AS rss_feed_runtime_status),
                    etag = COALESCE(EXCLUDED.etag, rss_feed_runtime.etag),
                    last_feed_update = COALESCE(EXCLUDED.last_feed_update, rss_feed_runtime.last_feed_update),
                    last_article_published_at = COALESCE(
                        EXCLUDED.last_article_published_at,
                        rss_feed_runtime.last_article_published_at
                    ),
                    consecutive_error_count = CASE
                        WHEN EXCLUDED.last_status = 'error'
                            THEN rss_feed_runtime.consecutive_error_count + 1
                        ELSE 0
                    END,
                    last_error_at = CASE
                        WHEN EXCLUDED.last_status = 'error'
                            THEN now()
                        ELSE NULL
                    END,
                    last_error_code = CASE
                        WHEN EXCLUDED.last_status = 'error'
                            THEN EXCLUDED.last_error_code
                        ELSE NULL
                    END
                """
            ),
            {
                "feed_id": result.feed_id,
                "status": result.status,
                "etag": result.new_etag,
                "last_feed_update": result.new_last_update,
                "last_article_published_at": _resolve_latest_source_published_at(result),
                "error_code": result.status_code,
            },
        )


def _resolve_latest_source_published_at(result: WorkerResultSchema) -> datetime | None:
    published_values: list[datetime] = []
    for source in result.sources:
        if source.published_at is not None:
            published_values.append(normalize_datetime_to_utc(source.published_at))
    if not published_values:
        return None
    return max(published_values)
