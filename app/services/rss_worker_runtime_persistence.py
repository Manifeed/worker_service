from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.workers.worker_result_schema import WorkerResultSchema
from shared_backend.utils.datetime_utils import normalize_datetime_to_utc


def upsert_rss_feed_runtime(
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
                "last_article_published_at": resolve_latest_source_published_at(result),
                "error_code": result.status_code,
            },
        )


def resolve_latest_source_published_at(result: WorkerResultSchema) -> datetime | None:
    published_values: list[datetime] = []
    for source in result.sources:
        if source.published_at is not None:
            published_values.append(normalize_datetime_to_utc(source.published_at))
    if not published_values:
        return None
    return max(published_values)
