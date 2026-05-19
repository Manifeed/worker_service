from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.clients.database.worker_job_models import WorkerJobTaskRecord, coerce_ref_ids


def get_worker_task_record(db: Session, *, task_id: int) -> WorkerJobTaskRecord | None:
    row = (
        db.execute(
            text(
                """
                SELECT
                    task.task_id,
                    task.execution_id,
                    task.job_id,
                    task.task_type,
                    task.status,
                    task.claim_expires_at,
                    task.worker_version,
                    task.ref_ids,
                    task.item_total,
                    task.attempt_count,
                    task.last_error,
                    task.claim_owner
                FROM worker_tasks AS task
                WHERE task.task_id = :task_id
                """
            ),
            {"task_id": task_id},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return WorkerJobTaskRecord(
        task_id=int(row["task_id"]),
        execution_id=int(row["execution_id"] or 0),
        job_id=str(row["job_id"]),
        task_type=str(row["task_type"]),
        status=str(row["status"]),
        claim_expires_at=row["claim_expires_at"],
        worker_version=str(row["worker_version"]) if row["worker_version"] is not None else None,
        ref_ids=coerce_ref_ids(row["ref_ids"]),
        item_total=int(row["item_total"] or 0),
        attempt_count=int(row["attempt_count"] or 0),
        last_error=str(row["last_error"]) if row["last_error"] is not None else None,
        claim_owner=str(row["claim_owner"]) if row["claim_owner"] is not None else None,
    )


def mark_worker_task_completed(
    db: Session,
    *,
    task_id: int,
    execution_id: int,
    trace_id: str | None,
    lease_id: str | None,
    item_success: int,
    item_error: int,
) -> bool:
    del trace_id, lease_id
    result = db.execute(
        text(
            """
            UPDATE worker_tasks
            SET
                status = 'completed',
                claimed_at = NULL,
                claim_expires_at = NULL,
                completed_at = now(),
                item_success = :item_success,
                item_error = :item_error,
                last_error = NULL,
                claim_owner = NULL
            WHERE task_id = :task_id
                AND execution_id = :execution_id
                AND status = 'processing'
            """
        ),
        {
            "task_id": task_id,
            "execution_id": execution_id,
            "item_success": max(0, int(item_success)),
            "item_error": max(0, int(item_error)),
        },
    )
    return result.rowcount > 0


def mark_worker_task_failed(
    db: Session,
    *,
    task_id: int,
    execution_id: int,
    trace_id: str | None,
    lease_id: str | None,
    error_message: str,
    item_error: int,
) -> bool:
    del trace_id, lease_id
    result = db.execute(
        text(
            """
            UPDATE worker_tasks
            SET
                status = 'failed',
                claimed_at = NULL,
                claim_expires_at = NULL,
                completed_at = now(),
                item_success = 0,
                item_error = :item_error,
                last_error = :last_error,
                claim_owner = NULL
            WHERE task_id = :task_id
                AND execution_id = :execution_id
                AND status = 'processing'
            """
        ),
        {
            "task_id": task_id,
            "execution_id": execution_id,
            "item_error": max(0, int(item_error)),
            "last_error": error_message[:2000],
        },
    )
    return result.rowcount > 0
