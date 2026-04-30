from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared_backend.schemas.jobs.job_schema import JobStatusRead


QUEUE_NAME_RSS_SCRAPE_REQUESTS = "rss.fetch"
QUEUE_NAME_SOURCE_EMBEDDING_REQUESTS = "embed.source"

TASK_KIND_RSS_SCRAPE = "rss_scrape"
TASK_KIND_SOURCE_EMBEDDING = "source_embedding"

WORKER_TYPE_RSS_SCRAPPER = "rss_scrapper"
WORKER_TYPE_SOURCE_EMBEDDING = "source_embedding"


@dataclass(frozen=True)
class WorkerJobTaskClaimRow:
    task_id: int
    execution_id: int
    job_id: str
    requested_at: datetime
    payload: dict[str, Any]
    worker_version: str | None
    task_type: str


@dataclass(frozen=True)
class WorkerJobTaskRecord:
    task_id: int
    execution_id: int
    job_id: str
    status: str
    claim_expires_at: datetime | None
    worker_version: str | None
    payload: dict[str, Any]
    item_total: int


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
        text(  # nosec
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
    if job_id is None:
        return None
    return str(job_id)


def enqueue_worker_tasks(
    db: Session,
    *,
    job_id: str,
    task_type: str,
    worker_version: str | None,
    requested_at: datetime,
    payloads: list[dict[str, Any]],
    item_counts: list[int],
) -> int:
    if not payloads:
        return 0
    if len(payloads) != len(item_counts):
        raise ValueError("payloads and item_counts length mismatch")
    db.execute(
        text(
            """
            INSERT INTO worker_tasks (
                job_id,
                task_type,
                worker_version,
                payload,
                requested_at,
                status,
                attempt_count,
                item_total,
                item_success,
                item_error
            ) VALUES (
                :job_id,
                :task_type,
                :worker_version,
                CAST(:payload AS JSONB),
                :requested_at,
                'pending',
                0,
                :item_total,
                0,
                0
            )
            """
        ),
        [
            {
                "job_id": job_id,
                "task_type": task_type,
                "worker_version": worker_version,
                "payload": json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
                "requested_at": requested_at,
                "item_total": max(0, int(item_count)),
            }
            for payload, item_count in zip(payloads, item_counts, strict=True)
        ],
    )
    return len(payloads)


def claim_worker_tasks(
    db: Session,
    *,
    task_type: str,
    worker_version: str | None,
    task_count: int,
    lease_seconds: int,
) -> list[WorkerJobTaskClaimRow]:
    filters = [
        "task.task_type = :task_type",
        "(task.status = 'pending' OR (task.status = 'processing' AND task.claim_expires_at IS NOT NULL AND task.claim_expires_at < now()))",
    ]
    params: dict[str, object] = {
        "task_type": task_type,
        "task_count": max(1, int(task_count)),
        "lease_seconds": max(30, int(lease_seconds)),
    }
    if worker_version is not None:
        filters.append("COALESCE(task.worker_version, '') = :worker_version")
        params["worker_version"] = worker_version

    rows = (
        db.execute(
            text(  # nosec
                """
                WITH candidate AS (
                    SELECT task.task_id
                    FROM worker_tasks AS task
                    WHERE """
                + " AND ".join(filters)
                + """
                    ORDER BY task.requested_at ASC, task.task_id ASC
                    LIMIT :task_count
                    FOR UPDATE SKIP LOCKED
                ),
                claimed AS (
                    UPDATE worker_tasks AS task
                    SET
                        status = 'processing',
                claimed_at = now(),
                claim_expires_at = now() + (:lease_seconds * interval '1 second'),
                attempt_count = task.attempt_count + 1,
                execution_id = nextval('worker_task_execution_id_seq')
            FROM candidate
            WHERE task.task_id = candidate.task_id
            RETURNING
                task.task_id,
                task.execution_id,
                task.job_id,
                task.requested_at,
                task.payload,
                        task.worker_version,
                        task.task_type
                )
                SELECT
                    claimed.task_id,
                    claimed.execution_id,
                    claimed.job_id,
                    claimed.requested_at,
                    claimed.payload,
                    claimed.worker_version,
                    claimed.task_type
                FROM claimed
                ORDER BY claimed.task_id ASC
                """
            ),
            params,
        )
        .mappings()
        .all()
    )
    return [
        WorkerJobTaskClaimRow(
            task_id=int(row["task_id"]),
            execution_id=int(row["execution_id"]),
            job_id=str(row["job_id"]),
            requested_at=row["requested_at"],
            payload=dict(row["payload"] or {}),
            worker_version=(
                str(row["worker_version"])
                if row["worker_version"] is not None
                else None
            ),
            task_type=str(row["task_type"]),
        )
        for row in rows
    ]


def get_worker_task_record(
    db: Session,
    *,
    task_id: int,
) -> WorkerJobTaskRecord | None:
    row = (
        db.execute(
            text(
                """
                SELECT
                    task.task_id,
                    task.execution_id,
                    task.job_id,
                    task.status,
                    task.claim_expires_at,
                    task.worker_version,
                    task.payload,
                    task.item_total
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
        status=str(row["status"]),
        claim_expires_at=row["claim_expires_at"],
        worker_version=(
            str(row["worker_version"])
            if row["worker_version"] is not None
            else None
        ),
        payload=dict(row["payload"] or {}),
        item_total=int(row["item_total"] or 0),
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
                item_error = :item_error
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
    del trace_id, lease_id, error_message
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
                item_error = :item_error
            WHERE task_id = :task_id
                AND execution_id = :execution_id
                AND status = 'processing'
            """
        ),
        {
            "task_id": task_id,
            "execution_id": execution_id,
            "item_error": max(0, int(item_error)),
        },
    )
    return result.rowcount > 0


def refresh_worker_job_status(
    db: Session,
    *,
    job_id: str,
) -> None:
    row = (
        db.execute(
            text(
                """
                SELECT
                    COUNT(task.task_id) AS task_total,
                    COUNT(task.task_id) FILTER (WHERE task.status IN ('completed', 'failed')) AS task_processed,
                    COALESCE(SUM(task.item_success), 0) AS item_success,
                    COALESCE(SUM(task.item_error), 0) AS item_error,
                    COUNT(task.task_id) FILTER (WHERE task.status = 'processing') AS processing_count,
                    COUNT(task.task_id) FILTER (WHERE task.status = 'pending') AS pending_count
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
        return
    task_total = int(row["task_total"] or 0)
    task_processed = int(row["task_processed"] or 0)
    item_success = int(row["item_success"] or 0)
    item_error = int(row["item_error"] or 0)
    processing_count = int(row["processing_count"] or 0)
    pending_count = int(row["pending_count"] or 0)
    if task_total == 0:
        status = "completed"
    elif processing_count > 0 or (task_processed > 0 and pending_count > 0):
        status = "processing"
    elif pending_count == task_total:
        status = "queued"
    elif item_error > 0:
        status = "completed_with_errors"
    else:
        status = "completed"
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
                    WHEN CAST(:status AS VARCHAR(64)) IN ('processing', 'completed', 'completed_with_errors')
                        THEN COALESCE(started_at, now())
                    ELSE started_at
                END,
                finished_at = CASE
                    WHEN CAST(:status AS VARCHAR(64)) IN ('completed', 'completed_with_errors', 'failed')
                        THEN COALESCE(finished_at, now())
                    ELSE NULL
                END,
                finalized_at = CASE
                    WHEN CAST(:status AS VARCHAR(64)) IN ('completed', 'completed_with_errors', 'failed')
                        THEN COALESCE(finalized_at, now())
                    ELSE NULL
                END
            WHERE job_id = :job_id
            """
        ),
        {
            "job_id": job_id,
            "status": status,
            "task_total": task_total,
            "task_processed": task_processed,
            "item_success": item_success,
            "item_error": item_error,
        },
    )


def get_worker_job_status_read(
    db: Session,
    *,
    job_id: str,
) -> JobStatusRead | None:
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
        worker_version=(
            str(row["worker_version"])
            if row["worker_version"] is not None
            else None
        ),
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


def list_worker_jobs(
    db: Session,
    *,
    limit: int,
) -> list[dict[str, Any]]:
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


def list_worker_job_tasks(
    db: Session,
    *,
    job_id: str,
) -> list[dict[str, Any]]:
    rows = (
        db.execute(
            text(
                """
                SELECT
                    task_id,
                    status,
                    claimed_at,
                    completed_at,
                    claim_expires_at,
                    item_total,
                    item_success,
                    item_error
                FROM worker_tasks
                WHERE job_id = :job_id
                ORDER BY task_id ASC
                """
            ),
            {"job_id": job_id},
        )
        .mappings()
        .all()
    )
    return [dict(row) for row in rows]
