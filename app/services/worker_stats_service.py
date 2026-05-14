from __future__ import annotations

from sqlalchemy.orm import Session

from app.clients.database.worker_gateway_database_client import count_active_worker_sessions
from app.clients.database.worker_job_database_client import (
    QUEUE_NAME_RSS_SCRAPE_REQUESTS,
    QUEUE_NAME_SOURCE_EMBEDDING_REQUESTS,
    RUNTIME_COUNTER_EMBEDDING_TASKS_REQUEUED,
    RUNTIME_COUNTER_PAYLOAD_REBUILD_FAILURES,
    RUNTIME_COUNTER_STALE_REDIS_TASK_IDS_DROPPED,
    count_expired_worker_claims,
    count_pending_worker_tasks,
    read_worker_runtime_counter_values,
)
from shared_backend.schemas.internal.worker_service_schema import WorkerServiceStatsRead


def read_worker_stats(workers_db: Session) -> WorkerServiceStatsRead:
    runtime_counters = read_worker_runtime_counter_values(workers_db)
    return WorkerServiceStatsRead(
        connected_workers=count_active_worker_sessions(workers_db),
        pending_rss_tasks=count_pending_worker_tasks(
            workers_db,
            task_type=QUEUE_NAME_RSS_SCRAPE_REQUESTS,
        ),
        pending_embedding_tasks=count_pending_worker_tasks(
            workers_db,
            task_type=QUEUE_NAME_SOURCE_EMBEDDING_REQUESTS,
        ),
        expired_claims=count_expired_worker_claims(workers_db),
        stale_redis_task_ids_dropped=runtime_counters[RUNTIME_COUNTER_STALE_REDIS_TASK_IDS_DROPPED],
        embedding_tasks_requeued=runtime_counters[RUNTIME_COUNTER_EMBEDDING_TASKS_REQUEUED],
        payload_rebuild_failures=runtime_counters[RUNTIME_COUNTER_PAYLOAD_REBUILD_FAILURES],
    )
