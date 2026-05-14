from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.domain.article_authors import normalize_article_author_name
from app.services.rss_worker_ingestion_candidates import (
    CandidateRow,
    upsert_article_url_variants,
)


def merge_candidates_into_articles(
    db: Session,
    *,
    candidate_rows: list[CandidateRow],
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
        _sync_article_authors(db, article_id=article_id, author_names=candidate_row.authors)
        _upsert_article_feed_link(db, article_id=article_id, feed_id=candidate_row.feed_id)
        upsert_article_url_variants(db, article_id=article_id, raw_urls=candidate_row.source_urls)


def _upsert_article(
    db: Session,
    *,
    candidate_row: CandidateRow,
) -> tuple[int, str]:
    existing_article = _find_existing_article(db, candidate_row=candidate_row)
    if existing_article is not None:
        existing_article_id, existing_article_key = existing_article
        _update_article_from_candidate(db, article_id=existing_article_id, candidate_row=candidate_row)
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
                        country,
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
                        :country,
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
                    "country": candidate_row.country or "xx",
                    "company_id": candidate_row.company_id,
                },
            ).scalar_one()
    except IntegrityError:
        existing_article = _find_existing_article(db, candidate_row=candidate_row)
        if existing_article is None:
            raise
        existing_article_id, existing_article_key = existing_article
        _update_article_from_candidate(db, article_id=existing_article_id, candidate_row=candidate_row)
        return existing_article_id, existing_article_key

    if article_id is None:
        raise RuntimeError("Expected inserted article_id after article upsert")
    return int(article_id), candidate_row.article_key


def _find_existing_article(
    db: Session,
    *,
    candidate_row: CandidateRow,
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


def _update_article_from_candidate(
    db: Session,
    *,
    article_id: int,
    candidate_row: CandidateRow,
) -> None:
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
                company_id = COALESCE(:company_id, articles.company_id),
                country = COALESCE(NULLIF(:country, ''), articles.country, 'xx')
            WHERE article_id = :article_id
            """
        ),
        {
            "article_id": article_id,
            "published_at": candidate_row.published_at,
            "canonical_url": candidate_row.canonical_url,
            "title": candidate_row.title,
            "summary": candidate_row.summary,
            "image_url": candidate_row.image_url,
            "content_key": candidate_row.content_key,
            "company_id": candidate_row.company_id,
            "country": candidate_row.country,
        },
    )


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
    db.execute(text("DELETE FROM article_authors WHERE article_id = :article_id"), {"article_id": article_id})
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


def _upsert_article_feed_link(db: Session, *, article_id: int, feed_id: int) -> None:
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
