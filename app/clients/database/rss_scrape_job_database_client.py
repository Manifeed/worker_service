from __future__ import annotations

from collections.abc import Sequence
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.domain.rss_catalog_normalization import normalize_host
from shared_backend.schemas.rss.rss_scrape_job_schema import RssScrapeFeedPayloadSchema
from shared_backend.utils.datetime_utils import normalize_datetime_to_utc

def list_rss_feed_scrape_payloads_by_ordered_ids(
    db: Session,
    *,
    feed_ids: Sequence[int],
) -> list[RssScrapeFeedPayloadSchema]:
    normalized_ids = [
        int(feed_id)
        for feed_id in feed_ids
        if isinstance(feed_id, int) and int(feed_id) > 0
    ]
    if not normalized_ids:
        return []
    rows = (
        db.execute(
            text(
                """
                WITH requested_feed_ids AS (
                    SELECT requested.feed_id, requested.position
                    FROM unnest(CAST(:feed_ids AS BIGINT[])) WITH ORDINALITY AS requested(feed_id, position)
                )
                SELECT
                    requested_feed_ids.feed_id,
                    feed.url AS feed_url,
                    feed.company_id,
                    company.host AS company_host,
                    COALESCE(company.fetchprotection, 1) AS fetchprotection,
                    runtime.etag,
                    runtime.last_feed_update,
                    runtime.last_article_published_at
                FROM requested_feed_ids
                LEFT JOIN rss_feeds AS feed
                    ON feed.id = requested_feed_ids.feed_id
                LEFT JOIN rss_company AS company
                    ON company.id = feed.company_id
                LEFT JOIN rss_feed_runtime AS runtime
                    ON runtime.feed_id = feed.id
                ORDER BY requested_feed_ids.position ASC
                """
            ),
            {"feed_ids": normalized_ids},
        )
        .mappings()
        .all()
    )
    return [
        RssScrapeFeedPayloadSchema(
            feed_id=int(row["feed_id"]),
            feed_url=str(row["feed_url"]),
            company_id=(int(row["company_id"]) if row["company_id"] is not None else None),
            host_header=normalize_host(row["company_host"]),
            fetchprotection=int(row["fetchprotection"] or 1),
            etag=(str(row["etag"]) if row["etag"] is not None else None),
            last_update=normalize_datetime_to_utc(row["last_feed_update"]),
            last_db_article_published_at=normalize_datetime_to_utc(row["last_article_published_at"]),
        )
        for row in rows
        if row["feed_url"] is not None
    ]
