from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.clients.database.worker_job_models import WorkerJobProgressSnapshot
from app.clients.database.worker_job_read_database_client import get_worker_job_progress_snapshot

def refresh_worker_job_status(db: Session, *, job_id: str) -> None:
    snapshot = get_worker_job_progress_snapshot(db, job_id=job_id)
    if snapshot is None:
        return
    _update_worker_job_status_row(
        db,
        job_id=job_id,
        status=_resolve_worker_job_status(snapshot),
        snapshot=snapshot,
    )


def _resolve_worker_job_status(snapshot: WorkerJobProgressSnapshot) -> str:
    if snapshot.task_total == 0:
        return "completed"
    if snapshot.processing_count > 0 or (
        snapshot.task_processed > 0 and snapshot.pending_count > 0
    ):
        return "processing"
    if snapshot.pending_count == snapshot.task_total or snapshot.pending_count > 0:
        return "queued"
    if snapshot.cancelled_count > 0:
        return "cancelled"
    if snapshot.item_error > 0:
        return "completed_with_errors"
    return "completed"


def _update_worker_job_status_row(
    db: Session,
    *,
    job_id: str,
    status: str,
    snapshot: WorkerJobProgressSnapshot,
) -> None:
    db.execute(
        text(
            """
            UPDATE worker_jobs
            SET
                status = CAST(:status AS VARCHAR(64)),
                task_total = :task_total,
                task_processed = :task_processed,
                item_success = :item_success,
                item_error = :item_error,
                started_at = CASE
                    WHEN CAST(:status AS VARCHAR(64)) IN ('processing', 'paused', 'cancelled', 'completed', 'completed_with_errors', 'failed')
                        THEN COALESCE(started_at, now())
                    ELSE started_at
                END,
                finished_at = CASE
                    WHEN CAST(:status AS VARCHAR(64)) IN ('cancelled', 'completed', 'completed_with_errors', 'failed')
                        THEN COALESCE(finished_at, now())
                    ELSE NULL
                END,
                finalized_at = CASE
                    WHEN CAST(:status AS VARCHAR(64)) IN ('cancelled', 'completed', 'completed_with_errors', 'failed')
                        THEN COALESCE(finalized_at, now())
                    ELSE NULL
                END
            WHERE job_id = :job_id
            """
        ),
        {
            "job_id": job_id,
            "status": status,
            "task_total": snapshot.task_total,
            "task_processed": snapshot.task_processed,
            "item_success": snapshot.item_success,
            "item_error": snapshot.item_error,
        },
    )
