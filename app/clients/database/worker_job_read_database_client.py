from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.clients.database.worker_job_models import (
    WorkerJobProgressSnapshot,
    WorkerJobRecord,
)
from shared_backend.schemas.jobs.job_schema import JobStatusRead


def get_active_worker_job_id(
    db: Session,
    *,
    job_kind: str,
    worker_version: str | None = None,
) -> str | None:
    filters = [
        "job.job_kind = :job_kind",
        "job.status IN ('queued', 'processing')",
    ]
    params: dict[str, object] = {"job_kind": job_kind}
    if worker_version is not None:
        filters.append("COALESCE(job.worker_version, '') = :worker_version")
        params["worker_version"] = worker_version
    job_id = db.execute(
        text(
            """
            SELECT job.job_id
            FROM worker_jobs AS job
            WHERE """
            + " AND ".join(filters)
            + """
            ORDER BY job.requested_at DESC, job.job_id DESC
            LIMIT 1
            """
        ),
        params,
    ).scalar_one_or_none()
    return str(job_id) if job_id is not None else None


def get_worker_job_record(db: Session, *, job_id: str) -> WorkerJobRecord | None:
    row = (
        db.execute(
            text(
                """
                SELECT
                    job_id,
                    job_kind,
                    task_type,
                    worker_version,
                    requested_at,
                    started_at,
                    finished_at,
                    finalized_at,
                    status,
                    task_total,
                    task_processed,
                    item_total,
                    item_success,
                    item_error
                FROM worker_jobs
                WHERE job_id = :job_id
                """
            ),
            {"job_id": job_id},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return WorkerJobRecord(
        job_id=str(row["job_id"]),
        job_kind=str(row["job_kind"]),
        task_type=str(row["task_type"]),
        worker_version=str(row["worker_version"]) if row["worker_version"] is not None else None,
        requested_at=row["requested_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        finalized_at=row["finalized_at"],
        status=str(row["status"]),
        task_total=int(row["task_total"] or 0),
        task_processed=int(row["task_processed"] or 0),
        item_total=int(row["item_total"] or 0),
        item_success=int(row["item_success"] or 0),
        item_error=int(row["item_error"] or 0),
    )


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


def get_worker_job_status_read(db: Session, *, job_id: str) -> JobStatusRead | None:
    row = (
        db.execute(
            text(
                """
                SELECT
                    job.job_id,
                    job.job_kind,
                    job.status,
                    job.worker_version,
                    job.requested_at,
                    job.started_at,
                    job.finished_at,
                    job.task_total,
                    job.task_processed,
                    job.item_total,
                    job.item_success,
                    job.item_error,
                    job.finalized_at
                FROM worker_jobs AS job
                WHERE job.job_id = :job_id
                """
            ),
            {"job_id": job_id},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return JobStatusRead(
        job_id=str(row["job_id"]),
        job_kind=str(row["job_kind"]),  # type: ignore[arg-type]
        status=str(row["status"]),  # type: ignore[arg-type]
        worker_version=str(row["worker_version"]) if row["worker_version"] is not None else None,
        requested_at=row["requested_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        task_total=int(row["task_total"] or 0),
        task_processed=int(row["task_processed"] or 0),
        item_total=int(row["item_total"] or 0),
        item_success=int(row["item_success"] or 0),
        item_error=int(row["item_error"] or 0),
        finalized_at=row["finalized_at"],
    )


def list_worker_jobs(db: Session, *, limit: int) -> list[dict[str, Any]]:
    rows = (
        db.execute(
            text(
                """
                SELECT
                    job_id,
                    job_kind,
                    status,
                    worker_version,
                    requested_at,
                    started_at,
                    finished_at,
                    task_total,
                    task_processed,
                    item_total,
                    item_success,
                    item_error,
                    finalized_at
                FROM worker_jobs
                ORDER BY requested_at DESC, job_id DESC
                LIMIT :limit
                """
            ),
            {"limit": max(1, min(int(limit), 500))},
        )
        .mappings()
        .all()
    )
    return [dict(row) for row in rows]
