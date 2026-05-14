from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.clients.database.worker_job_models import WorkerJobProgressSnapshot
from app.clients.database.worker_job_read_database_client import get_worker_job_progress_snapshot


def create_worker_job(
    db: Session,
    *,
    job_id: str,
    job_kind: str,
    task_type: str,
    worker_version: str | None,
    requested_at: datetime,
    status: str,
    task_total: int,
    item_total: int,
) -> None:
    db.execute(
        text(
            """
            INSERT INTO worker_jobs (
                job_id,
                job_kind,
                task_type,
                worker_version,
                requested_at,
                status,
                task_total,
                task_processed,
                item_total,
                item_success,
                item_error
            ) VALUES (
                :job_id,
                :job_kind,
                :task_type,
                :worker_version,
                :requested_at,
                :status,
                :task_total,
                0,
                :item_total,
                0,
                0
            )
            """
        ),
        {
            "job_id": job_id,
            "job_kind": job_kind,
            "task_type": task_type,
            "worker_version": worker_version,
            "requested_at": requested_at,
            "status": status,
            "task_total": max(0, int(task_total)),
            "item_total": max(0, int(item_total)),
        },
    )


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


def set_worker_job_status(db: Session, *, job_id: str, status: str) -> bool:
    snapshot = get_worker_job_progress_snapshot(db, job_id=job_id)
    if snapshot is None:
        return False
    _update_worker_job_status_row(
        db,
        job_id=job_id,
        status=status,
        snapshot=snapshot,
    )
    return True


def requeue_processing_tasks_for_job(db: Session, *, job_id: str) -> int:
    result = db.execute(
        text(
            """
            UPDATE worker_tasks
            SET
                status = 'pending',
                claimed_at = NULL,
                claim_expires_at = NULL,
                claim_owner = NULL
            WHERE job_id = :job_id
                AND status = 'processing'
            """
        ),
        {"job_id": job_id},
    )
    return int(result.rowcount or 0)


def cancel_active_tasks_for_job(db: Session, *, job_id: str) -> int:
    result = db.execute(
        text(
            """
            UPDATE worker_tasks
            SET
                status = 'cancelled',
                claimed_at = NULL,
                claim_expires_at = NULL,
                completed_at = COALESCE(completed_at, now()),
                claim_owner = NULL
            WHERE job_id = :job_id
                AND status IN ('pending', 'processing')
            """
        ),
        {"job_id": job_id},
    )
    return int(result.rowcount or 0)


def delete_worker_job(db: Session, *, job_id: str) -> bool:
    result = db.execute(
        text(
            """
            DELETE FROM worker_jobs
            WHERE job_id = :job_id
            """
        ),
        {"job_id": job_id},
    )
    return result.rowcount > 0


def clear_admin_job_automation_references(db: Session, *, job_id: str) -> int:
    result = db.execute(
        text(
            """
            UPDATE admin_job_automation_settings
            SET
                current_ingest_job_id = CASE
                    WHEN current_ingest_job_id = :job_id THEN NULL
                    ELSE current_ingest_job_id
                END,
                current_embed_job_id = CASE
                    WHEN current_embed_job_id = :job_id THEN NULL
                    ELSE current_embed_job_id
                END,
                updated_at = now()
            WHERE current_ingest_job_id = :job_id
                OR current_embed_job_id = :job_id
            """
        ),
        {"job_id": job_id},
    )
    return int(result.rowcount or 0)


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
