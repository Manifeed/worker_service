from __future__ import annotations

from sqlalchemy.orm import Session

from app.errors.custom_exceptions import (
    WorkerProtocolError,
)
from app.schemas.workers.worker_rss_result_schema import WorkerRssTaskResultPayloadSchema
from app.services.rss_worker_ingestion_service import persist_rss_task_results
from app.services.worker_task_finalization_service import (
    complete_claimed_worker_task,
    fail_claimed_worker_task,
    require_claimed_worker_task,
)


def complete_rss_task(
    content_db: Session,
    workers_db: Session,
    *,
    task_id: int,
    execution_id: int,
    worker_name: str,
    trace_id: str,
    lease_id: str,
    result_payload: WorkerRssTaskResultPayloadSchema,
) -> str | None:
    task = require_claimed_worker_task(
        workers_db,
        task_id=task_id,
        execution_id=execution_id,
        task_label="RSS",
    )

    expected_feed_ids = {
        int(feed["feed_id"])
        for feed in task.payload.get("feeds", [])
        if isinstance(feed, dict) and feed.get("feed_id") is not None
    }
    seen_feed_ids: set[int] = set()
    for result_event in result_payload.result_events:
        if result_event.job_id != task.payload.get("job_id"):
            raise WorkerProtocolError(
                f"RSS task {task_id} completed with unexpected job_id {result_event.job_id}"
            )
        if result_event.feed_id not in expected_feed_ids:
            raise WorkerProtocolError(
                f"RSS task {task_id} completed unexpected feed_id {result_event.feed_id}"
            )
        if result_event.feed_id in seen_feed_ids:
            raise WorkerProtocolError(
                f"RSS task {task_id} completed feed_id {result_event.feed_id} twice"
            )
        seen_feed_ids.add(result_event.feed_id)
    missing_feed_ids = sorted(expected_feed_ids - seen_feed_ids)
    if missing_feed_ids:
        raise WorkerProtocolError(
            f"RSS task {task_id} is missing results for feed_ids {missing_feed_ids}"
        )

    persist_rss_task_results(
        content_db,
        trace_id=trace_id,
        lease_id=lease_id,
        worker_name=worker_name,
        local_dedup=result_payload.local_dedup,
        results=result_payload.result_events,
    )
    success_feed_count = sum(
        1 for result in result_payload.result_events if result.status != "error"
    )
    error_feed_count = sum(
        1 for result in result_payload.result_events if result.status == "error"
    )
    return complete_claimed_worker_task(
        workers_db,
        task=task,
        trace_id=trace_id,
        lease_id=lease_id,
        item_success=success_feed_count,
        item_error=error_feed_count,
        task_label="RSS",
    )


def fail_rss_task(
    db: Session,
    *,
    task_id: int,
    execution_id: int,
    trace_id: str,
    lease_id: str,
    error_message: str,
) -> str | None:
    task = require_claimed_worker_task(
        db,
        task_id=task_id,
        execution_id=execution_id,
        task_label="RSS",
    )
    return fail_claimed_worker_task(
        db,
        task=task,
        trace_id=trace_id,
        lease_id=lease_id,
        error_message=error_message,
        item_error=task.item_total,
        task_label="RSS",
    )
