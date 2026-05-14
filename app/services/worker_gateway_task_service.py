from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.clients.database.rss_scrape_job_database_client import list_rss_feed_scrape_payloads_by_ordered_ids
from app.clients.database.worker_job_database_client import (
    RUNTIME_COUNTER_PAYLOAD_REBUILD_FAILURES,
    claim_worker_tasks as claim_worker_task_rows,
    increment_worker_runtime_counter,
    mark_worker_task_failed,
    refresh_worker_job_status,
)
from app.schemas.workers.worker_gateway_schema import (
    WorkerTaskClaimRequestSchema,
    WorkerTaskCompleteRequestSchema,
    WorkerTaskFailRequestSchema,
)
from app.services.rss_worker_task_service import complete_rss_task, fail_rss_task
from app.services.worker_auth_service import AuthenticatedWorkerContext
from app.services.worker_gateway_signature_service import (
    is_rss_task_type,
    validate_rss_worker_result_payload,
)
from shared_backend.errors.custom_exceptions import WorkerProtocolError


@dataclass(frozen=True)
class ClaimedTaskRead:
    task_id: int
    execution_id: int
    payload: dict[str, Any]


def claim_tasks(
    content_db: Session,
    db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    payload: WorkerTaskClaimRequestSchema,
) -> list[ClaimedTaskRead]:
    claimed_rows = claim_worker_task_rows(
        db,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
        task_count=payload.count,
        lease_seconds=payload.lease_seconds,
        claim_owner=worker.worker_name,
    )
    claimed_tasks: list[ClaimedTaskRead] = []
    for row in claimed_rows:
        if is_rss_task_type(row.task_type):
            rss_payload = rebuild_rss_claim_payload(content_db, row)
            if rss_payload is None:
                increment_worker_runtime_counter(
                    db,
                    counter_name=RUNTIME_COUNTER_PAYLOAD_REBUILD_FAILURES,
                )
                mark_worker_task_failed(
                    db,
                    task_id=row.task_id,
                    execution_id=row.execution_id,
                    trace_id=None,
                    lease_id=None,
                    error_message="stale_reference: unable to rebuild RSS payload from feed refs",
                    item_error=row.item_total,
                )
                refresh_worker_job_status(db, job_id=row.job_id)
                continue
            claimed_tasks.append(
                ClaimedTaskRead(
                    task_id=row.task_id,
                    execution_id=row.execution_id,
                    payload=rss_payload,
                )
            )
            continue
        raise WorkerProtocolError(f"Unsupported worker task type: {row.task_type}")
    return claimed_tasks


def complete_task(
    content_db: Session,
    workers_db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    payload: WorkerTaskCompleteRequestSchema,
    task_id: int,
    execution_id: int,
) -> str | None:
    if is_rss_task_type(payload.task_type):
        rss_result_payload = validate_rss_worker_result_payload(payload.result_payload)
        return complete_rss_task(
            content_db,
            workers_db,
            worker_name=worker.worker_name,
            task_id=task_id,
            execution_id=execution_id,
            trace_id=payload.trace_id,
            lease_id=payload.lease_id,
            result_payload=rss_result_payload,
        )
    raise WorkerProtocolError(f"Unsupported worker task type: {payload.task_type}")


def fail_task(
    db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    payload: WorkerTaskFailRequestSchema,
    task_id: int,
    execution_id: int,
) -> str | None:
    if is_rss_task_type(payload.task_type):
        return fail_rss_task(
            db,
            task_id=task_id,
            execution_id=execution_id,
            trace_id=payload.trace_id,
            lease_id=payload.lease_id,
            error_message=payload.error_message,
        )
    raise WorkerProtocolError(f"Unsupported worker task type: {payload.task_type}")


def rebuild_rss_claim_payload(
    content_db: Session,
    claimed_task: object,
) -> dict[str, Any] | None:
    row = claimed_task
    feed_ids = getattr(row, "ref_ids", [])
    if not feed_ids:
        return None
    feeds = list_rss_feed_scrape_payloads_by_ordered_ids(content_db, feed_ids=feed_ids)
    if len(feeds) != len(feed_ids):
        return None
    if [int(feed.feed_id) for feed in feeds] != [int(feed_id) for feed_id in feed_ids]:
        return None
    return {
        "job_id": getattr(row, "job_id"),
        "requested_at": getattr(row, "requested_at").isoformat(),
        "ingest": True,
        "requested_by": "jobs.rss_scrape",
        "feeds": [feed.model_dump(mode="json") for feed in feeds],
    }
