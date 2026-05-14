from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.clients.database.worker_job_models import WorkerJobTaskClaimRow, coerce_ref_ids


def enqueue_worker_tasks(
    db: Session,
    *,
    job_id: str,
    task_type: str,
    worker_version: str | None,
    requested_at: datetime,
    ref_batches: list[list[int]],
    item_counts: list[int],
) -> list[int]:
    if not ref_batches:
        return []
    if len(ref_batches) != len(item_counts):
        raise ValueError("ref_batches and item_counts length mismatch")
    task_ids: list[int] = []
    for ref_batch, item_count in zip(ref_batches, item_counts, strict=True):
        normalized_ref_ids = [int(value) for value in ref_batch if int(value) > 0]
        if not normalized_ref_ids:
            raise ValueError("worker task ref batch cannot be empty")
        task_id = db.execute(
            text(
                """
                INSERT INTO worker_tasks (
                    job_id,
                    task_type,
                    worker_version,
                    ref_ids,
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
                    CAST(:ref_ids AS BIGINT[]),
                    :requested_at,
                    'pending',
                    0,
                    :item_total,
                    0,
                    0
                )
                RETURNING task_id
                """
            ),
            {
                "job_id": job_id,
                "task_type": task_type,
                "worker_version": worker_version,
                "ref_ids": normalized_ref_ids,
                "requested_at": requested_at,
                "item_total": max(1, int(item_count)),
            },
        ).scalar_one()
        task_ids.append(int(task_id))
    return task_ids


def claim_worker_tasks(
    db: Session,
    *,
    task_type: str,
    worker_version: str | None,
    task_count: int,
    lease_seconds: int,
    claim_owner: str,
) -> list[WorkerJobTaskClaimRow]:
    filters = [
        "task.task_type = :task_type",
        "(task.status = 'pending' OR (task.status = 'processing' AND task.claim_expires_at IS NOT NULL AND task.claim_expires_at < now()))",
    ]
    params: dict[str, object] = {
        "task_type": task_type,
        "task_count": max(1, int(task_count)),
        "lease_seconds": max(30, int(lease_seconds)),
        "claim_owner": claim_owner[:255],
    }
    if worker_version is not None:
        filters.append("COALESCE(task.worker_version, '') = :worker_version")
        params["worker_version"] = worker_version
    rows = (
        db.execute(
            text(
                """
                WITH candidate AS (
                    SELECT task.task_id
                    FROM worker_tasks AS task
                    JOIN worker_jobs AS job ON job.job_id = task.job_id
                    WHERE """
                + " AND ".join(filters)
                + """
                    AND job.status IN ('queued', 'processing')
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
                        completed_at = NULL,
                        attempt_count = task.attempt_count + 1,
                        execution_id = nextval('worker_task_execution_id_seq'),
                        last_error = NULL,
                        claim_owner = :claim_owner
                    FROM candidate
                    WHERE task.task_id = candidate.task_id
                    RETURNING
                        task.task_id,
                        task.execution_id,
                        task.job_id,
                        task.requested_at,
                        task.ref_ids,
                        task.worker_version,
                        task.task_type,
                        task.item_total
                )
                SELECT
                    claimed.task_id,
                    claimed.execution_id,
                    claimed.job_id,
                    claimed.requested_at,
                    claimed.ref_ids,
                    claimed.worker_version,
                    claimed.task_type,
                    claimed.item_total
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
            ref_ids=coerce_ref_ids(row["ref_ids"]),
            worker_version=str(row["worker_version"]) if row["worker_version"] is not None else None,
            task_type=str(row["task_type"]),
            item_total=int(row["item_total"] or 0),
        )
        for row in rows
    ]
