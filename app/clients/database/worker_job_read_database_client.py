from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.clients.database.worker_job_models import WorkerJobProgressSnapshot


def get_worker_job_progress_snapshot(
    db: Session,
    *,
    job_id: str,
) -> WorkerJobProgressSnapshot | None:
    row = (
        db.execute(
            text(
                """
                SELECT
                    COUNT(task.task_id) AS task_total,
                    COUNT(task.task_id) FILTER (WHERE task.status IN ('completed', 'failed', 'cancelled')) AS task_processed,
                    COALESCE(SUM(task.item_success), 0) AS item_success,
                    COALESCE(SUM(task.item_error), 0) AS item_error,
                    COUNT(task.task_id) FILTER (WHERE task.status = 'processing') AS processing_count,
                    COUNT(task.task_id) FILTER (WHERE task.status = 'pending') AS pending_count,
                    COUNT(task.task_id) FILTER (WHERE task.status = 'cancelled') AS cancelled_count
                FROM worker_tasks AS task
                WHERE task.job_id = :job_id
                """
            ),
            {"job_id": job_id},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return WorkerJobProgressSnapshot(
        task_total=int(row["task_total"] or 0),
        task_processed=int(row["task_processed"] or 0),
        item_success=int(row["item_success"] or 0),
        item_error=int(row["item_error"] or 0),
        processing_count=int(row["processing_count"] or 0),
        pending_count=int(row["pending_count"] or 0),
        cancelled_count=int(row["cancelled_count"] or 0),
    )
