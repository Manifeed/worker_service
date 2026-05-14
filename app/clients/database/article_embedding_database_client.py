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
    country: str
    published_at: datetime | None
    feeds: list[dict[str, object]]
    authors: list[dict[str, object]]
    img_url: str | None


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


def list_article_ids_without_embeddings(
    db: Session,
    *,
    model_name: str,
    reembed_model_mismatches: bool = False,
) -> list[int]:
    del reembed_model_mismatches
    rows = (
        db.execute(
            text(
                """
                SELECT article.article_id
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
        .all()
    )
    return [int(row[0]) for row in rows]


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
                    COALESCE(NULLIF(article.country, ''), 'xx') AS country,
                    article.published_at,
                    COALESCE(
                        (
                            SELECT json_agg(
                                json_build_object(
                                    'id', feed.id,
                                    'section', feed.section
                                )
                                ORDER BY feed.id
                            )
                            FROM article_feed_links AS link
                            JOIN rss_feeds AS feed
                                ON feed.id = link.feed_id
                            WHERE link.article_id = article.article_id
                        ),
                        '[]'::json
                    ) AS feeds,
                    COALESCE(
                        (
                            SELECT json_agg(
                                json_build_object(
                                    'id', author.id,
                                    'name', COALESCE(author.display_name, '')
                                )
                                ORDER BY article_author.position
                            )
                            FROM article_authors AS article_author
                            JOIN authors AS author
                                ON author.id = article_author.author_id
                            WHERE article_author.article_id = article.article_id
                        ),
                        '[]'::json
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
            country=(str(row["country"]) if row["country"] is not None else "xx"),
            published_at=row["published_at"],
            feeds=[dict(feed) for feed in (row["feeds"] or []) if isinstance(feed, dict)],
            authors=[dict(author_row) for author_row in (row["authors"] or []) if isinstance(author_row, dict)],
            img_url=(str(row["image_url"]) if row["image_url"] is not None else None),
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
