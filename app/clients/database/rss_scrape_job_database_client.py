from __future__ import annotations

from collections.abc import Sequence
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.domain.rss_catalog_normalization import normalize_host
from shared_backend.schemas.rss.rss_scrape_job_schema import RssScrapeFeedPayloadSchema
from shared_backend.utils.datetime_utils import normalize_datetime_to_utc


def list_rss_feed_scrape_payloads(
    db: Session,
    *,
    feed_ids: Sequence[int] | None = None,
    enabled_only: bool = False,
) -> list[RssScrapeFeedPayloadSchema]:
    filters: list[str] = []
    params: dict[str, object] = {}
    if enabled_only:
        filters.append('feed.enabled = TRUE')
        filters.append('COALESCE(company.enabled, TRUE) = TRUE')
    if feed_ids:
        normalized_ids = sorted({feed_id for feed_id in feed_ids if isinstance(feed_id, int) and feed_id > 0})
        if not normalized_ids:
            return []
        filters.append('feed.id = ANY(:feed_ids)')
        params['feed_ids'] = normalized_ids
    where_sql = ''
    if filters:
        where_sql = 'WHERE ' + ' AND '.join(filters)
    rows = (
        db.execute(
            text(  # nosec
                f"""
                SELECT
                    feed.id AS feed_id,
                    feed.url AS feed_url,
                    feed.company_id,
                    company.host AS company_host,
                    COALESCE(company.fetchprotection, 1) AS fetchprotection,
                    runtime.etag,
                    runtime.last_feed_update,
                    runtime.last_article_published_at
                FROM rss_feeds AS feed
                LEFT JOIN rss_company AS company
                    ON company.id = feed.company_id
                LEFT JOIN rss_feed_runtime AS runtime
                    ON runtime.feed_id = feed.id
                {where_sql}
                ORDER BY feed.id ASC
                """
            ),
            params,
        )
        .mappings()
        .all()
    )
    return [
        RssScrapeFeedPayloadSchema(
            feed_id=int(row['feed_id']),
            feed_url=str(row['feed_url']),
            company_id=(int(row['company_id']) if row['company_id'] is not None else None),
            host_header=normalize_host(row['company_host']),
            fetchprotection=int(row['fetchprotection'] or 1),
            etag=(str(row['etag']) if row['etag'] is not None else None),
            last_update=normalize_datetime_to_utc(row['last_feed_update']),
            last_db_article_published_at=normalize_datetime_to_utc(row['last_article_published_at']),
        )
        for row in rows
    ]
