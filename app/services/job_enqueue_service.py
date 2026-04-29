from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.errors.custom_exceptions import JobAlreadyRunningError, JobEnqueueError
from app.domain.job_lock import JobAlreadyRunning, job_lock
from app.clients.database.rss_scrape_job_database_client import list_rss_feed_scrape_payloads
from app.domain.rss_scrape_batching import build_rss_scrape_batches
from app.schemas.enums import WorkerJobKind, WorkerJobStatus
from app.schemas.jobs.job_enqueue_schema import JobEnqueueRead
from app.schemas.sources.source_embedding_schema import RssSourceEmbeddingPayloadSchema
from app.clients.database.article_embedding_database_client import list_articles_without_embeddings
from app.domain.source_embedding_config import (
    FIXED_SOURCE_EMBEDDING_MODEL_NAME,
)
from app.services.worker_version_service import (
    resolve_rss_worker_version,
    resolve_source_embedding_worker_version,
)
from app.clients.database.worker_job_database_client import (
    create_worker_job,
    enqueue_worker_tasks,
    get_active_worker_job_id,
)

JOB_ID_TIMESTAMP_FORMAT = "%Y-%m-%d-%H-%M-%S"


def enqueue_rss_scrape_job(
    content_db: Session,
    workers_db: Session | None = None,
    *,
    feed_ids: list[int] | None = None,
    commit: bool = True,
) -> JobEnqueueRead:
    workers_db = workers_db or content_db
    try:
        with job_lock(workers_db, "rss_ingest"):
            return _enqueue_rss_scrape_job(
                content_db,
                workers_db,
                feed_ids=feed_ids,
                commit=commit,
            )
    except JobAlreadyRunning as exception:
        raise JobAlreadyRunningError("RSS scrape job creation already running") from exception


def enqueue_source_embedding_job(
    content_db: Session,
    workers_db: Session | None = None,
    *,
    reembed_model_mismatches: bool = False,
    commit: bool = True,
) -> JobEnqueueRead:
    workers_db = workers_db or content_db
    try:
        with job_lock(workers_db, "sources_enqueue_embeddings"):
            return _enqueue_source_embedding_job(
                content_db,
                workers_db,
                reembed_model_mismatches=reembed_model_mismatches,
                commit=commit,
            )
    except JobAlreadyRunning as exception:
        raise JobAlreadyRunningError("Source embedding enqueue already running") from exception


def _enqueue_rss_scrape_job(
    content_db: Session,
    workers_db: Session,
    *,
    feed_ids: list[int] | None,
    commit: bool,
) -> JobEnqueueRead:
    worker_version = resolve_rss_worker_version()
    active_job_id = get_active_worker_job_id(
        workers_db,
        job_kind=WorkerJobKind.RSS_SCRAPE.value,
    )
    if active_job_id is not None:
        raise JobAlreadyRunningError(f"RSS scrape job {active_job_id} is not finished yet")

    normalized_feed_ids = _normalize_requested_feed_ids(feed_ids)
    feeds = list_rss_feed_scrape_payloads(
        content_db,
        feed_ids=normalized_feed_ids,
        enabled_only=True,
    )
    requested_at = datetime.now(timezone.utc)
    job_id = _build_job_id("rss", requested_at)
    feed_batches = build_rss_scrape_batches(
        feeds,
        batch_size=20,
        random_seed=job_id,
    )
    tasks_total = len(feed_batches)
    status = (
        WorkerJobStatus.COMPLETED
        if not feeds
        else WorkerJobStatus.QUEUED
    )

    try:
        create_worker_job(
            workers_db,
            job_id=job_id,
            job_kind=WorkerJobKind.RSS_SCRAPE.value,
            task_type="rss.fetch",
            worker_version=worker_version,
            requested_at=requested_at,
            status=status.value,
            task_total=tasks_total,
            item_total=len(feeds),
        )
        if feeds:
            enqueue_worker_tasks(
                workers_db,
                job_id=job_id,
                task_type="rss.fetch",
                worker_version=worker_version,
                requested_at=requested_at,
                payloads=[
                    {
                        "job_id": job_id,
                        "requested_at": requested_at.isoformat(),
                        "ingest": True,
                        "requested_by": "jobs.rss_scrape",
                        "feeds": [feed.model_dump(mode="json") for feed in batch],
                    }
                    for batch in feed_batches
                ],
                item_counts=[len(batch) for batch in feed_batches],
            )
        if commit:
            workers_db.commit()
    except Exception as exception:
        workers_db.rollback()
        raise JobEnqueueError(f"Unable to enqueue RSS scrape job: {exception}") from exception

    return JobEnqueueRead(
        job_id=job_id,
        job_kind=WorkerJobKind.RSS_SCRAPE,
        status=status,
        worker_version=worker_version,
        tasks_total=tasks_total,
        items_total=len(feeds),
    )


def _enqueue_source_embedding_job(
    content_db: Session,
    workers_db: Session,
    *,
    reembed_model_mismatches: bool,
    commit: bool,
) -> JobEnqueueRead:
    worker_version = resolve_source_embedding_worker_version()
    active_job_id = get_active_worker_job_id(
        workers_db,
        job_kind=WorkerJobKind.SOURCE_EMBEDDING.value,
        worker_version=worker_version,
    )
    if active_job_id is not None:
        raise JobAlreadyRunningError(
            f"Embedding job {active_job_id} for worker version {worker_version} is not finished yet"
        )

    candidates = list_articles_without_embeddings(
        content_db,
        worker_version=worker_version,
        reembed_model_mismatches=reembed_model_mismatches,
    )
    requested_at = datetime.now(timezone.utc)
    job_id = _build_job_id("emb", requested_at)
    batches = [
        candidates[start : start + 128]
        for start in range(0, len(candidates), 128)
    ]
    tasks_total = len(batches)
    status = (
        WorkerJobStatus.COMPLETED
        if not candidates
        else WorkerJobStatus.QUEUED
    )

    try:
        create_worker_job(
            workers_db,
            job_id=job_id,
            job_kind=WorkerJobKind.SOURCE_EMBEDDING.value,
            task_type="embed.source",
            worker_version=worker_version,
            requested_at=requested_at,
            status=status.value,
            task_total=tasks_total,
            item_total=len(candidates),
        )
        if candidates:
            enqueue_worker_tasks(
                workers_db,
                job_id=job_id,
                task_type="embed.source",
                worker_version=worker_version,
                requested_at=requested_at,
                payloads=[
                    {
                        "job_id": job_id,
                        "worker_version": worker_version,
                        "embedding_model_name": FIXED_SOURCE_EMBEDDING_MODEL_NAME,
                        "sources": [
                            RssSourceEmbeddingPayloadSchema(
                                id=candidate.article_id,
                                title=candidate.title,
                                summary=candidate.summary,
                                url=candidate.url,
                            ).model_dump(mode="json")
                            for candidate in batch
                        ],
                    }
                    for batch in batches
                ],
                item_counts=[len(batch) for batch in batches],
            )
        if commit:
            workers_db.commit()
    except Exception as exception:
        workers_db.rollback()
        raise JobEnqueueError(f"Unable to enqueue source embedding job: {exception}") from exception

    return JobEnqueueRead(
        job_id=job_id,
        job_kind=WorkerJobKind.SOURCE_EMBEDDING,
        status=status,
        worker_version=worker_version,
        tasks_total=tasks_total,
        items_total=len(candidates),
    )


def _normalize_requested_feed_ids(feed_ids: list[int] | None) -> list[int] | None:
    if feed_ids is None:
        return None
    return sorted(set(feed_ids))


def _build_job_id(prefix: str, requested_at: datetime) -> str:
    normalized_requested_at = requested_at.astimezone(timezone.utc)
    return f"{prefix}-{normalized_requested_at.strftime(JOB_ID_TIMESTAMP_FORMAT)}"
