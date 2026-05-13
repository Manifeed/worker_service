from __future__ import annotations

from sqlalchemy.orm import Session

from shared_backend.errors.custom_exceptions import JobNotFoundError, JobStateError
from shared_backend.schemas.jobs.job_schema import JobControlCommandRead, JobStatusRead
from app.clients.database.worker_job_database_client import (
    cancel_active_tasks_for_job,
    clear_admin_job_automation_references,
    delete_worker_job,
    get_worker_job_record,
    get_worker_job_status_read,
    refresh_worker_job_status,
    requeue_processing_tasks_for_job,
    set_worker_job_status,
)

_FINISHED_JOB_STATUSES = {
    "cancelled",
    "completed",
    "completed_with_errors",
    "failed",
}
_RUNNABLE_JOB_STATUSES = {"queued", "processing"}


def pause_job(
    db: Session,
    *,
    job_id: str,
) -> JobStatusRead:
    job = _require_job_record(db, job_id=job_id)
    if job.status == "paused":
        return _require_job_status_read(db, job_id=job_id)
    if job.status not in _RUNNABLE_JOB_STATUSES:
        raise JobStateError(f"Job {job_id} cannot be paused from status {job.status}")

    try:
        requeue_processing_tasks_for_job(db, job_id=job_id)
        set_worker_job_status(db, job_id=job_id, status="paused")
        db.commit()
    except Exception:
        db.rollback()
        raise
    return _require_job_status_read(db, job_id=job_id)


def resume_job(
    db: Session,
    *,
    job_id: str,
) -> JobStatusRead:
    job = _require_job_record(db, job_id=job_id)
    if job.status in _RUNNABLE_JOB_STATUSES:
        return _require_job_status_read(db, job_id=job_id)
    if job.status != "paused":
        raise JobStateError(f"Job {job_id} cannot be resumed from status {job.status}")

    try:
        refresh_worker_job_status(db, job_id=job_id)
        db.commit()
    except Exception:
        db.rollback()
        raise
    return _require_job_status_read(db, job_id=job_id)


def cancel_job(
    db: Session,
    *,
    job_id: str,
) -> JobStatusRead:
    job = _require_job_record(db, job_id=job_id)
    if job.status == "cancelled":
        return _require_job_status_read(db, job_id=job_id)
    if job.status in _FINISHED_JOB_STATUSES:
        raise JobStateError(f"Job {job_id} cannot be cancelled from status {job.status}")

    try:
        cancel_active_tasks_for_job(db, job_id=job_id)
        set_worker_job_status(db, job_id=job_id, status="cancelled")
        clear_admin_job_automation_references(db, job_id=job_id)
        db.commit()
    except Exception:
        db.rollback()
        raise
    return _require_job_status_read(db, job_id=job_id)


def delete_job_permanently(
    db: Session,
    *,
    job_id: str,
) -> JobControlCommandRead:
    _require_job_record(db, job_id=job_id)
    try:
        clear_admin_job_automation_references(db, job_id=job_id)
        delete_worker_job(db, job_id=job_id)
        db.commit()
    except Exception:
        db.rollback()
        raise
    return JobControlCommandRead(
        ok=True,
        job_id=job_id,
        status=None,
        deleted=True,
    )


def _require_job_record(db: Session, *, job_id: str):
    job = get_worker_job_record(db, job_id=job_id)
    if job is None:
        raise JobNotFoundError(f"Job {job_id} not found")
    return job


def _require_job_status_read(
    db: Session,
    *,
    job_id: str,
) -> JobStatusRead:
    job_status = get_worker_job_status_read(db, job_id=job_id)
    if job_status is None:
        raise JobNotFoundError(f"Job {job_id} not found")
    return job_status
