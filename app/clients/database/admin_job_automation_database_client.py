from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared_backend.utils.datetime_utils import normalize_datetime_to_utc


SETTINGS_KEY = "default"
DEFAULT_INTERVAL_MINUTES = 30


@dataclass(frozen=True)
class JobAutomationSettingsRecord:
    enabled: bool
    interval_minutes: int
    last_cycle_started_at: datetime | None
    current_ingest_job_id: str | None
    current_embed_job_id: str | None


def get_or_create_job_automation_settings(
    workers_db: Session,
) -> tuple[JobAutomationSettingsRecord, bool]:
    insert_result = workers_db.execute(
        text(
            """
            INSERT INTO admin_job_automation_settings (
                singleton_key,
                enabled,
                interval_minutes,
                last_cycle_started_at,
                current_ingest_job_id,
                current_embed_job_id
            ) VALUES (
                :singleton_key,
                false,
                :interval_minutes,
                NULL,
                NULL,
                NULL
            )
            ON CONFLICT (singleton_key) DO NOTHING
            """
        ),
        {
            "singleton_key": SETTINGS_KEY,
            "interval_minutes": DEFAULT_INTERVAL_MINUTES,
        },
    )
    row = (
        workers_db.execute(
            text(
                """
                SELECT
                    enabled,
                    interval_minutes,
                    last_cycle_started_at,
                    current_ingest_job_id,
                    current_embed_job_id
                FROM admin_job_automation_settings
                WHERE singleton_key = :singleton_key
                """
            ),
            {"singleton_key": SETTINGS_KEY},
        )
        .mappings()
        .one()
    )
    return map_job_automation_settings_record(row), bool(insert_result.rowcount)


def update_job_automation_enabled(
    workers_db: Session,
    *,
    enabled: bool,
) -> JobAutomationSettingsRecord:
    row = (
        workers_db.execute(
            text(
                """
                UPDATE admin_job_automation_settings
                SET
                    enabled = :enabled,
                    last_cycle_started_at = NULL,
                    current_ingest_job_id = NULL,
                    current_embed_job_id = NULL,
                    updated_at = now()
                WHERE singleton_key = :singleton_key
                RETURNING
                    enabled,
                    interval_minutes,
                    last_cycle_started_at,
                    current_ingest_job_id,
                    current_embed_job_id
                """
            ),
            {
                "singleton_key": SETTINGS_KEY,
                "enabled": enabled,
            },
        )
        .mappings()
        .one()
    )
    return map_job_automation_settings_record(row)


def update_job_automation_runtime_fields(
    workers_db: Session,
    *,
    last_cycle_started_at: datetime | None,
    current_ingest_job_id: str | None,
    current_embed_job_id: str | None,
) -> None:
    workers_db.execute(
        text(
            """
            UPDATE admin_job_automation_settings
            SET
                last_cycle_started_at = :last_cycle_started_at,
                current_ingest_job_id = :current_ingest_job_id,
                current_embed_job_id = :current_embed_job_id,
                updated_at = now()
            WHERE singleton_key = :singleton_key
            """
        ),
        {
            "singleton_key": SETTINGS_KEY,
            "last_cycle_started_at": last_cycle_started_at,
            "current_ingest_job_id": current_ingest_job_id,
            "current_embed_job_id": current_embed_job_id,
        },
    )


def map_job_automation_settings_record(row) -> JobAutomationSettingsRecord:
    return JobAutomationSettingsRecord(
        enabled=bool(row["enabled"]),
        interval_minutes=max(1, int(row["interval_minutes"] or DEFAULT_INTERVAL_MINUTES)),
        last_cycle_started_at=normalize_datetime_to_utc(row["last_cycle_started_at"]),
        current_ingest_job_id=str(row["current_ingest_job_id"]) if row["current_ingest_job_id"] is not None else None,
        current_embed_job_id=str(row["current_embed_job_id"]) if row["current_embed_job_id"] is not None else None,
    )
