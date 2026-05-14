from __future__ import annotations

from sqlalchemy.orm import Session

from app.schemas.workers.worker_result_schema import WorkerResultSchema
from app.schemas.workers.worker_rss_result_schema import WorkerRssTaskLocalDedupSchema
from app.services.rss_worker_article_persistence import merge_candidates_into_articles
from app.services.rss_worker_ingestion_candidates import (
    build_candidate_rows,
    load_feed_contexts,
    normalize_article_image_url,
)
from app.services.rss_worker_runtime_persistence import upsert_rss_feed_runtime


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
    feed_contexts = load_feed_contexts(
        db,
        feed_ids=[result.feed_id for result in results],
    )
    candidate_rows = build_candidate_rows(
        db,
        feed_contexts=feed_contexts,
        results=results,
    )
    merge_candidates_into_articles(db, candidate_rows=candidate_rows)
    upsert_rss_feed_runtime(db, results=results)


_normalize_article_image_url = normalize_article_image_url
