from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.clients.database.worker_job_models import KNOWN_RUNTIME_COUNTERS


def count_pending_worker_tasks(db: Session, *, task_type: str) -> int:
    return int(
        db.execute(
            text(
                """
                SELECT COUNT(*)
                FROM worker_tasks
                WHERE task_type = :task_type
                    AND status = 'pending'
                """
            ),
            {"task_type": task_type},
        ).scalar_one()
        or 0
    )


def count_expired_worker_claims(db: Session) -> int:
    return int(
        db.execute(
            text(
                """
                SELECT COUNT(*)
                FROM worker_tasks AS task
                JOIN worker_jobs AS job
                    ON job.job_id = task.job_id
                WHERE task.status = 'processing'
                    AND task.claim_expires_at IS NOT NULL
                    AND task.claim_expires_at < now()
                    AND job.status IN ('queued', 'processing')
                """
            )
        ).scalar_one()
        or 0
    )


def increment_worker_runtime_counter(
    db: Session,
    *,
    counter_name: str,
    amount: int = 1,
) -> None:
    normalized_amount = int(amount)
    if counter_name not in KNOWN_RUNTIME_COUNTERS or normalized_amount <= 0:
        return
    db.execute(
        text(
            """
            INSERT INTO worker_runtime_counters (
                counter_name,
                counter_value,
                updated_at
            ) VALUES (
                :counter_name,
                :counter_value,
                now()
            )
            ON CONFLICT (counter_name) DO UPDATE SET
                counter_value = worker_runtime_counters.counter_value + EXCLUDED.counter_value,
                updated_at = now()
            """
        ),
        {
            "counter_name": counter_name,
            "counter_value": normalized_amount,
        },
    )


def read_worker_runtime_counter_values(db: Session) -> dict[str, int]:
    rows = (
        db.execute(
            text(
                """
                SELECT counter_name, counter_value
                FROM worker_runtime_counters
                WHERE counter_name = ANY(:counter_names)
                """
            ),
            {"counter_names": list(KNOWN_RUNTIME_COUNTERS)},
        )
        .mappings()
        .all()
    )
    values = {name: 0 for name in KNOWN_RUNTIME_COUNTERS}
    for row in rows:
        values[str(row["counter_name"])] = int(row["counter_value"] or 0)
    return values
