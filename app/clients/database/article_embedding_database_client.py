from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.domain.source_identity import normalize_source_url


@dataclass(frozen=True)
class ArticleEmbeddingCandidateRead:
    article_id: int
    article_key: str
    title: str
    summary: str | None
    url: str


@dataclass(frozen=True)
class ArticleEmbeddingIndexRead:
    article_id: int
    article_key: str
    url: str
    title: str
    summary: str | None
    company_id: int | None
    company: str | None
    language: str | None
    published_at: datetime | None
    feed_ids: list[int]
    feeds: list[dict[str, object]]
    author_ids: list[int]
    authors: list[str]
    images_url: list[str]


def list_articles_without_embeddings(
    db: Session,
    *,
    model_name: str,
    reembed_model_mismatches: bool = False,
) -> list[ArticleEmbeddingCandidateRead]:
    del reembed_model_mismatches
    rows = (
        db.execute(
            text(
                """
                SELECT
                    article.article_id,
                    article.article_key,
                    article.title,
                    article.summary,
                    COALESCE(
                        NULLIF(article.canonical_url, ''),
                        'article://' || article.article_key
                    ) AS canonical_url
                FROM articles AS article
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM embedding_manifest AS manifest
                    WHERE manifest.article_id = article.article_id
                        AND manifest.model_name = :model_name
                        AND manifest.status = 'indexed'
                )
                ORDER BY article.article_id ASC
                """
            ),
            {"model_name": model_name},
        )
        .mappings()
        .all()
    )
    return [
        ArticleEmbeddingCandidateRead(
            article_id=int(row["article_id"]),
            article_key=str(row["article_key"]),
            title=_resolve_article_title(row["title"], row["article_key"]),
            summary=(str(row["summary"]) if row["summary"] is not None else None),
            url=str(row["canonical_url"]),
        )
        for row in rows
    ]


def count_indexed_embeddings(
    db: Session,
    *,
    model_name: str | None = None,
) -> int:
    if model_name is None:
        return int(
            db.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM embedding_manifest
                    WHERE status = 'indexed'
                    """
                )
            ).scalar_one()
        )
    return int(
        db.execute(
            text(
                """
                SELECT COUNT(*)
                FROM embedding_manifest
                WHERE status = 'indexed'
                    AND model_name = :model_name
                """
            ),
            {"model_name": model_name},
        ).scalar_one()
    )


def get_article_key_by_id(db: Session, *, article_id: int) -> str | None:
    article_key = db.execute(
        text(
            """
            SELECT article_key
            FROM articles
            WHERE article_id = :article_id
            """
        ),
        {"article_id": article_id},
    ).scalar_one_or_none()
    return str(article_key) if article_key is not None else None


def get_article_embedding_index_reads(
    db: Session,
    *,
    article_ids: list[int],
) -> dict[int, ArticleEmbeddingIndexRead]:
    if not article_ids:
        return {}
    rows = (
        db.execute(
            text(
                """
                SELECT
                    article.article_id,
                    article.article_key,
                    COALESCE(
                        NULLIF(article.canonical_url, ''),
                        'article://' || article.article_key
                    ) AS url,
                    COALESCE(NULLIF(article.title, ''), article.article_key) AS title,
                    article.summary,
                    article.image_url,
                    article.company_id,
                    company.name AS company,
                    article.language,
                    article.published_at,
                    COALESCE(
                        (
                            SELECT array_agg(link.feed_id ORDER BY link.feed_id)
                            FROM article_feed_links AS link
                            WHERE link.article_id = article.article_id
                        ),
                        ARRAY[]::integer[]
                    ) AS feed_ids,
                    COALESCE(
                        (
                            SELECT json_agg(
                                json_build_object(
                                    'id', feed.id,
                                    'url', feed.url,
                                    'section', feed.section,
                                    'company_id', feed.company_id,
                                    'company', feed_company.name
                                )
                                ORDER BY feed.id
                            )
                            FROM article_feed_links AS link
                            JOIN rss_feeds AS feed
                                ON feed.id = link.feed_id
                            LEFT JOIN rss_company AS feed_company
                                ON feed_company.id = feed.company_id
                            WHERE link.article_id = article.article_id
                        ),
                        '[]'::json
                    ) AS feeds,
                    COALESCE(
                        (
                            SELECT array_agg(author.id ORDER BY article_author.position)
                            FROM article_authors AS article_author
                            JOIN authors AS author
                                ON author.id = article_author.author_id
                            WHERE article_author.article_id = article.article_id
                        ),
                        ARRAY[]::bigint[]
                    ) AS author_ids,
                    COALESCE(
                        (
                            SELECT array_agg(author.display_name ORDER BY article_author.position)
                            FROM article_authors AS article_author
                            JOIN authors AS author
                                ON author.id = article_author.author_id
                            WHERE article_author.article_id = article.article_id
                        ),
                        ARRAY[]::text[]
                    ) AS authors
                FROM articles AS article
                LEFT JOIN rss_company AS company
                    ON company.id = article.company_id
                WHERE article.article_id = ANY(:article_ids)
                """
            ),
            {"article_ids": article_ids},
        )
        .mappings()
        .all()
    )
    return {
        int(row["article_id"]): ArticleEmbeddingIndexRead(
            article_id=int(row["article_id"]),
            article_key=str(row["article_key"]),
            url=str(row["url"]),
            title=str(row["title"]),
            summary=(str(row["summary"]) if row["summary"] is not None else None),
            company_id=(
                int(row["company_id"])
                if row["company_id"] is not None
                else None
            ),
            company=(str(row["company"]) if row["company"] is not None else None),
            language=(str(row["language"]) if row["language"] is not None else None),
            published_at=row["published_at"],
            feed_ids=[int(feed_id) for feed_id in (row["feed_ids"] or [])],
            feeds=[dict(feed) for feed in (row["feeds"] or []) if isinstance(feed, dict)],
            author_ids=[int(author_id) for author_id in (row["author_ids"] or [])],
            authors=[str(author) for author in (row["authors"] or []) if author],
            images_url=(
                [str(row["image_url"])]
                if row["image_url"] is not None
                else []
            ),
        )
        for row in rows
    }


def build_article_embedding_source_checksum(
    candidate: ArticleEmbeddingCandidateRead,
) -> str:
    payload = "||".join(
        [
            candidate.article_key,
            candidate.title,
            candidate.summary or "",
            normalize_source_url(candidate.url),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _resolve_article_title(
    title: object,
    article_key: object,
) -> str:
    raw_title = (str(title).strip() if title is not None else "")
    if raw_title:
        return raw_title
    return str(article_key)
