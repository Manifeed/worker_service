from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.errors.custom_exceptions import JobNotFoundError
from app.schemas.jobs.job_schema import (
    JobOverviewItemRead,
    JobStatusRead,
    JobTaskRead,
    JobsOverviewRead,
)
from app.clients.database.worker_job_database_client import (
    get_worker_job_status_read,
    list_worker_job_tasks,
    list_worker_jobs,
)


def list_jobs(
    db: Session,
    *,
    limit: int = 100,
) -> JobsOverviewRead:
    rows = list_worker_jobs(db, limit=limit)
    return JobsOverviewRead(
        generated_at=datetime.now(timezone.utc),
        items=[_build_job_overview_item_read(row) for row in rows],
    )


def get_job_status(
    db: Session,
    *,
    job_id: str,
) -> JobStatusRead:
    payload = get_worker_job_status_read(db, job_id=job_id)
    if payload is None:
        raise JobNotFoundError(f"Job {job_id} not found")
    return payload


def list_job_tasks(
    db: Session,
    *,
    job_id: str,
) -> list[JobTaskRead]:
    job = get_job_status(db, job_id=job_id)
    del job
    rows = list_worker_job_tasks(db, job_id=job_id)
    return [_build_job_task_read(row) for row in rows]


def _build_job_status_read(row) -> JobStatusRead:
    return JobStatusRead(
        job_id=str(row["job_id"]),
        job_kind=str(row["job_kind"]),  # type: ignore[arg-type]
        status=str(row["status"]),  # type: ignore[arg-type]
        requested_at=row["requested_at"],
        task_total=int(row["task_total"] or 0),
        task_processed=int(row["task_processed"] or 0),
        item_success=int(row["item_success"] or 0),
        item_error=int(row["item_error"] or 0),
        worker_version=(str(row["worker_version"]) if row["worker_version"] is not None else None),
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        item_total=int(row["item_total"] or 0),
        finalized_at=row["finalized_at"],
    )


def _build_job_overview_item_read(row) -> JobOverviewItemRead:
    return JobOverviewItemRead(
        job_id=str(row["job_id"]),
        job_kind=str(row["job_kind"]),  # type: ignore[arg-type]
        status=str(row["status"]),  # type: ignore[arg-type]
        requested_at=row["requested_at"],
        task_total=int(row["task_total"] or 0),
        task_processed=int(row["task_processed"] or 0),
        item_success=int(row["item_success"] or 0),
        item_error=int(row["item_error"] or 0),
    )


def _build_job_task_read(row) -> JobTaskRead:
    return JobTaskRead(
        task_id=int(row["task_id"]),
        status=str(row["status"]),  # type: ignore[arg-type]
        claimed_at=row["claimed_at"],
        completed_at=row["completed_at"],
        claim_expires_at=row["claim_expires_at"],
        item_total=int(row["item_total"] or 0),
        item_success=int(row["item_success"] or 0),
        item_error=int(row["item_error"] or 0),
    )
