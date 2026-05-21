from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.domain.language_detection import FastTextLikeDetector, detect_article_language


@dataclass(frozen=True)
class ArticleLanguageBackfillBatch:
    scanned_count: int
    updated_count: int
    last_article_id: int | None


def backfill_unknown_article_languages(
    db: Session,
    *,
    batch_size: int = 500,
    start_after_article_id: int = 0,
    detector: FastTextLikeDetector | None = None,
) -> ArticleLanguageBackfillBatch:
    rows = (
        db.execute(
            text(
                """
                SELECT article_id, country, title, summary, canonical_url
                FROM articles
                WHERE article_id > :start_after_article_id
                    AND COALESCE(NULLIF(language, ''), 'xx') = 'xx'
                ORDER BY article_id ASC
                LIMIT :batch_size
                """
            ),
            {
                "start_after_article_id": start_after_article_id,
                "batch_size": max(1, batch_size),
            },
        )
        .mappings()
        .all()
    )
    if not rows:
        return ArticleLanguageBackfillBatch(scanned_count=0, updated_count=0, last_article_id=None)

    updated_count = 0
    last_article_id: int | None = None
    for row in rows:
        article_id = int(row["article_id"])
        last_article_id = article_id
        language = detect_article_language(
            country=str(row["country"] or "xx"),
            title=str(row["title"] or ""),
            summary=str(row["summary"]) if row["summary"] is not None else None,
            urls=[str(row["canonical_url"])] if row.get("canonical_url") is not None else None,
            detector=detector,
        )
        if language == "xx":
            continue
        db.execute(
            text(
                """
                UPDATE articles
                SET language = :language
                WHERE article_id = :article_id
                    AND COALESCE(NULLIF(language, ''), 'xx') = 'xx'
                """
            ),
            {
                "article_id": article_id,
                "language": language,
            },
        )
        updated_count += 1
    return ArticleLanguageBackfillBatch(
        scanned_count=len(rows),
        updated_count=updated_count,
        last_article_id=last_article_id,
    )
