from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


QUEUE_NAME_RSS_SCRAPE_REQUESTS = "rss.fetch"
QUEUE_NAME_SOURCE_EMBEDDING_REQUESTS = "embed.source"

TASK_KIND_RSS_SCRAPE = "rss_scrape"
TASK_KIND_SOURCE_EMBEDDING = "source_embedding"

WORKER_TYPE_RSS_SCRAPPER = "rss_scrapper"
WORKER_TYPE_SOURCE_EMBEDDING = "source_embedding"

RUNTIME_COUNTER_STALE_REDIS_TASK_IDS_DROPPED = "stale_redis_task_ids_dropped"
RUNTIME_COUNTER_EMBEDDING_TASKS_REQUEUED = "embedding_tasks_requeued"
RUNTIME_COUNTER_PAYLOAD_REBUILD_FAILURES = "payload_rebuild_failures"
KNOWN_RUNTIME_COUNTERS = (
    RUNTIME_COUNTER_STALE_REDIS_TASK_IDS_DROPPED,
    RUNTIME_COUNTER_EMBEDDING_TASKS_REQUEUED,
    RUNTIME_COUNTER_PAYLOAD_REBUILD_FAILURES,
)


@dataclass(frozen=True)
class WorkerJobTaskClaimRow:
    task_id: int
    execution_id: int
    job_id: str
    requested_at: datetime
    ref_ids: list[int]
    worker_version: str | None
    task_type: str
    item_total: int


@dataclass(frozen=True)
class WorkerJobTaskRecord:
    task_id: int
    execution_id: int
    job_id: str
    task_type: str
    status: str
    claim_expires_at: datetime | None
    worker_version: str | None
    ref_ids: list[int]
    item_total: int
    attempt_count: int
    last_error: str | None
    claim_owner: str | None


@dataclass(frozen=True)
class WorkerJobRecord:
    job_id: str
    job_kind: str
    task_type: str
    worker_version: str | None
    requested_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    finalized_at: datetime | None
    status: str
    task_total: int
    task_processed: int
    item_total: int
    item_success: int
    item_error: int


@dataclass(frozen=True)
class WorkerJobProgressSnapshot:
    task_total: int
    task_processed: int
    item_success: int
    item_error: int
    processing_count: int
    pending_count: int
    cancelled_count: int


def coerce_ref_ids(raw_ref_ids: object) -> list[int]:
    if not isinstance(raw_ref_ids, list):
        return []
    normalized: list[int] = []
    for value in raw_ref_ids:
        if value is None:
            continue
        normalized_value = int(value)
        if normalized_value > 0:
            normalized.append(normalized_value)
    return normalized
