from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from shared_backend.errors.custom_exceptions import (
    WorkerTaskNotFoundError,
    WorkerTaskStateError,
)
from app.clients.database.worker_job_database_client import (
    WorkerJobTaskRecord,
    get_worker_task_record,
    mark_worker_task_completed,
    mark_worker_task_failed,
    refresh_worker_job_status,
)


@dataclass(frozen=True)
class ClaimedWorkerTaskContext:
    task_id: int
    execution_id: int
    job_id: str
    worker_version: str | None
    payload: dict
    item_total: int


def require_claimed_worker_task(
    db: Session,
    *,
    task_id: int,
    execution_id: int,
    task_label: str,
) -> ClaimedWorkerTaskContext:
    task_record = get_worker_task_record(db, task_id=task_id)
    if task_record is None:
        raise WorkerTaskNotFoundError(f"Missing {task_label} task {task_id}")
    _require_active_execution(task_record, execution_id=execution_id, task_label=task_label)
    return ClaimedWorkerTaskContext(
        task_id=task_record.task_id,
        execution_id=task_record.execution_id,
        job_id=task_record.job_id,
        worker_version=task_record.worker_version,
        payload=task_record.payload,
        item_total=task_record.item_total,
    )


def complete_claimed_worker_task(
    db: Session,
    *,
    task: ClaimedWorkerTaskContext,
    trace_id: str,
    lease_id: str,
    item_success: int,
    item_error: int,
    task_label: str,
) -> str:
    was_marked = mark_worker_task_completed(
        db,
        task_id=task.task_id,
        execution_id=task.execution_id,
        trace_id=trace_id,
        lease_id=lease_id,
        item_success=item_success,
        item_error=item_error,
    )
    if not was_marked:
        raise WorkerTaskStateError(
            f"{task_label} task {task.task_id} is no longer claimable for execution {task.execution_id}"
        )
    refresh_worker_job_status(db, job_id=task.job_id)
    return task.job_id


def fail_claimed_worker_task(
    db: Session,
    *,
    task: ClaimedWorkerTaskContext,
    trace_id: str,
    lease_id: str,
    error_message: str,
    item_error: int,
    task_label: str,
) -> str:
    was_marked = mark_worker_task_failed(
        db,
        task_id=task.task_id,
        execution_id=task.execution_id,
        trace_id=trace_id,
        lease_id=lease_id,
        error_message=error_message,
        item_error=item_error,
    )
    if not was_marked:
        raise WorkerTaskStateError(
            f"{task_label} task {task.task_id} is no longer claimable for execution {task.execution_id}"
        )
    refresh_worker_job_status(db, job_id=task.job_id)
    return task.job_id


def normalize_worker_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _require_active_execution(
    task_record: WorkerJobTaskRecord,
    *,
    execution_id: int,
    task_label: str,
) -> None:
    if task_record.execution_id != execution_id:
        raise WorkerTaskStateError(
            f"{task_label} task {task_record.task_id} execution {execution_id} no longer matches the active claim"
        )
    if task_record.status != "processing":
        raise WorkerTaskStateError(
            f"{task_label} task {task_record.task_id} is not currently processing"
        )
    if (
        task_record.claim_expires_at is not None
        and normalize_worker_datetime(task_record.claim_expires_at) < datetime.now(timezone.utc)
    ):
        raise WorkerTaskStateError(
            f"{task_label} task {task_record.task_id} claim expired before finalization"
        )
