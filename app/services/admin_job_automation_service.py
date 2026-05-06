from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Event, Lock, Thread
import logging
import os
import sys
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared_backend.errors.custom_exceptions import JobNotFoundError
from shared_backend.errors.custom_exceptions import JobAlreadyRunningError
from app.domain.job_lock import JobAlreadyRunning, job_lock
from shared_backend.schemas.enums import WorkerJobKind, WorkerKind
from shared_backend.schemas.jobs.job_automation_schema import (
    JobAutomationRead,
    JobAutomationUpdateRequestSchema,
)
from shared_backend.utils.datetime_utils import normalize_datetime_to_utc
from app.clients.database.worker_gateway_database_client import count_active_worker_sessions
from app.services.worker_version_service import resolve_source_embedding_worker_version
from database import open_content_read_db_session, open_workers_write_db_session

from app.services.job_enqueue_service import enqueue_rss_scrape_job, enqueue_source_embedding_job
from app.services.job_read_service import get_job_status
from app.clients.database.worker_job_database_client import get_active_worker_job_id

_SETTINGS_KEY = "default"
_DEFAULT_INTERVAL_MINUTES = 30
_SCHEDULER_POLL_SECONDS = 30
_SCHEDULER_GUARD = Lock()
_SCHEDULER_THREAD: Thread | None = None
_SCHEDULER_STOP_EVENT: Event | None = None
_LOGGER = logging.getLogger("app")
_FINISHED_JOB_STATUSES = {"cancelled", "completed", "completed_with_errors", "failed"}


@dataclass(frozen=True)
class JobAutomationSettingsRecord:
    enabled: bool
    interval_minutes: int
    last_cycle_started_at: datetime | None
    current_ingest_job_id: str | None
    current_embed_job_id: str | None


@dataclass(frozen=True)
class JobAutomationRuntimeSnapshot:
    status: str
    message: str
    connected_workers: int
    connected_rss_workers: int
    connected_embedding_workers: int
    next_run_at: datetime | None
    current_ingest_status: str | None
    current_embed_status: str | None


def read_job_automation(workers_db: Session) -> JobAutomationRead:
    settings, initialized = _get_or_create_settings(workers_db)
    if initialized:
        workers_db.commit()
        settings, _ = _get_or_create_settings(workers_db)
    if _reconcile_finished_cycle(workers_db, settings):
        workers_db.commit()
        settings, _ = _get_or_create_settings(workers_db)
    snapshot = _build_runtime_snapshot(workers_db, settings)
    return _build_job_automation_read(settings, snapshot)


def update_job_automation(
    workers_db: Session,
    payload: JobAutomationUpdateRequestSchema,
) -> JobAutomationRead:
    _, initialized = _get_or_create_settings(workers_db)
    if initialized:
        workers_db.commit()
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
                "singleton_key": _SETTINGS_KEY,
                "enabled": payload.enabled,
            },
        )
        .mappings()
        .one()
    )
    workers_db.commit()
    settings = _map_settings_record(row)
    snapshot = _build_runtime_snapshot(workers_db, settings)
    return _build_job_automation_read(settings, snapshot)


def start_admin_job_automation_scheduler() -> None:
    if _is_scheduler_disabled():
        return

    global _SCHEDULER_THREAD, _SCHEDULER_STOP_EVENT
    with _SCHEDULER_GUARD:
        if _SCHEDULER_THREAD is not None and _SCHEDULER_THREAD.is_alive():
            return
        stop_event = Event()
        thread = Thread(
            target=_scheduler_loop,
            args=(stop_event,),
            name="admin-job-automation",
            daemon=True,
        )
        _SCHEDULER_STOP_EVENT = stop_event
        _SCHEDULER_THREAD = thread
        thread.start()


def stop_admin_job_automation_scheduler() -> None:
    global _SCHEDULER_THREAD, _SCHEDULER_STOP_EVENT
    with _SCHEDULER_GUARD:
        stop_event = _SCHEDULER_STOP_EVENT
        thread = _SCHEDULER_THREAD
        _SCHEDULER_STOP_EVENT = None
        _SCHEDULER_THREAD = None
    if stop_event is not None:
        stop_event.set()
    if thread is not None and thread.is_alive():
        thread.join(timeout=2.0)


def run_admin_job_automation_tick() -> None:
    content_db = open_content_read_db_session()
    workers_db = open_workers_write_db_session()
    try:
        try:
            with job_lock(workers_db, "admin_job_automation_tick"):
                _run_locked_admin_job_automation_tick(content_db, workers_db)
        except JobAlreadyRunning:
            return
    finally:
        workers_db.close()
        content_db.close()


def _scheduler_loop(stop_event: Event) -> None:
    while not stop_event.is_set():
        try:
            run_admin_job_automation_tick()
        except Exception:
            _LOGGER.exception("Admin job automation tick failed")
        if stop_event.wait(_SCHEDULER_POLL_SECONDS):
            break


def _run_locked_admin_job_automation_tick(content_db: Session, workers_db: Session) -> None:
    settings, initialized = _get_or_create_settings(workers_db)
    settings_changed = initialized or _reconcile_finished_cycle(workers_db, settings)
    if settings_changed:
        workers_db.commit()
        settings, _ = _get_or_create_settings(workers_db)

    if not settings.enabled:
        return

    snapshot = _build_runtime_snapshot(workers_db, settings)
    try:
        if snapshot.status == "ready":
            cycle_started_at = datetime.now(timezone.utc)
            enqueue_result = enqueue_rss_scrape_job(
                content_db,
                workers_db,
                commit=False,
            )
            _update_runtime_fields(
                workers_db,
                last_cycle_started_at=cycle_started_at,
                current_ingest_job_id=enqueue_result.job_id,
                current_embed_job_id=None,
            )
            workers_db.commit()
            _LOGGER.info("Admin automation launched ingest job %s", enqueue_result.job_id)
            return

        if snapshot.status == "ready_for_embed":
            enqueue_result = enqueue_source_embedding_job(
                content_db,
                workers_db,
                commit=False,
            )
            _update_runtime_fields(
                workers_db,
                last_cycle_started_at=settings.last_cycle_started_at,
                current_ingest_job_id=settings.current_ingest_job_id,
                current_embed_job_id=enqueue_result.job_id,
            )
            workers_db.commit()
            _LOGGER.info("Admin automation launched embed job %s", enqueue_result.job_id)
            return
    except JobAlreadyRunningError:
        workers_db.rollback()
        return
    except Exception:
        workers_db.rollback()
        raise

    if settings_changed:
        workers_db.commit()


def _build_runtime_snapshot(
    workers_db: Session,
    settings: JobAutomationSettingsRecord,
) -> JobAutomationRuntimeSnapshot:
    now = datetime.now(timezone.utc)
    connected_rss_workers = count_active_worker_sessions(
        workers_db,
        worker_type=WorkerKind.RSS_SCRAPPER.value,
    )
    connected_embedding_workers = count_active_worker_sessions(
        workers_db,
        worker_type=WorkerKind.SOURCE_EMBEDDING.value,
    )
    connected_workers = connected_rss_workers + connected_embedding_workers
    current_ingest_status = _get_job_status_value(workers_db, settings.current_ingest_job_id)
    current_embed_status = _get_job_status_value(workers_db, settings.current_embed_job_id)
    active_rss_job_id = get_active_worker_job_id(
        workers_db,
        job_kind=WorkerJobKind.RSS_SCRAPE.value,
    )
    active_embedding_job_id = get_active_worker_job_id(
        workers_db,
        job_kind=WorkerJobKind.SOURCE_EMBEDDING.value,
        worker_version=resolve_source_embedding_worker_version(),
    )
    next_run_at = (
        _resolve_next_run_at(
            settings.last_cycle_started_at,
            settings.interval_minutes,
            now=now,
        )
        if settings.enabled
        else None
    )

    if not settings.enabled:
        return JobAutomationRuntimeSnapshot(
            status="disabled",
            message="Automation is disabled.",
            connected_workers=connected_workers,
            connected_rss_workers=connected_rss_workers,
            connected_embedding_workers=connected_embedding_workers,
            next_run_at=None,
            current_ingest_status=current_ingest_status,
            current_embed_status=current_embed_status,
        )

    if settings.current_embed_job_id:
        if not _is_finished_job_status(current_embed_status):
            return JobAutomationRuntimeSnapshot(
                status="running_embed",
                message="Embed job is running.",
                connected_workers=connected_workers,
                connected_rss_workers=connected_rss_workers,
                connected_embedding_workers=connected_embedding_workers,
                next_run_at=next_run_at,
                current_ingest_status=current_ingest_status,
                current_embed_status=current_embed_status,
            )

    if settings.current_ingest_job_id:
        if not _is_finished_job_status(current_ingest_status):
            return JobAutomationRuntimeSnapshot(
                status="running_ingest",
                message="Ingest job is running.",
                connected_workers=connected_workers,
                connected_rss_workers=connected_rss_workers,
                connected_embedding_workers=connected_embedding_workers,
                next_run_at=next_run_at,
                current_ingest_status=current_ingest_status,
                current_embed_status=current_embed_status,
            )
        if active_embedding_job_id is not None:
            return JobAutomationRuntimeSnapshot(
                status="waiting_embedding_job",
                message="Waiting for the current embedding job to finish before launching the automated embed.",
                connected_workers=connected_workers,
                connected_rss_workers=connected_rss_workers,
                connected_embedding_workers=connected_embedding_workers,
                next_run_at=next_run_at,
                current_ingest_status=current_ingest_status,
                current_embed_status=current_embed_status,
            )
        if connected_embedding_workers <= 0:
            return JobAutomationRuntimeSnapshot(
                status="waiting_embedding_workers",
                message="Waiting for a connected embedding worker to launch embed.",
                connected_workers=connected_workers,
                connected_rss_workers=connected_rss_workers,
                connected_embedding_workers=connected_embedding_workers,
                next_run_at=next_run_at,
                current_ingest_status=current_ingest_status,
                current_embed_status=current_embed_status,
            )
        return JobAutomationRuntimeSnapshot(
            status="ready_for_embed",
            message="Ingest finished. Embed can start on the next scheduler tick.",
            connected_workers=connected_workers,
            connected_rss_workers=connected_rss_workers,
            connected_embedding_workers=connected_embedding_workers,
            next_run_at=next_run_at,
            current_ingest_status=current_ingest_status,
            current_embed_status=current_embed_status,
        )

    if active_rss_job_id is not None:
        return JobAutomationRuntimeSnapshot(
            status="waiting_ingest_job",
            message="Waiting for the current ingest job to finish before starting a new automated cycle.",
            connected_workers=connected_workers,
            connected_rss_workers=connected_rss_workers,
            connected_embedding_workers=connected_embedding_workers,
            next_run_at=next_run_at,
            current_ingest_status=current_ingest_status,
            current_embed_status=current_embed_status,
        )

    if next_run_at is not None and now < next_run_at:
        return JobAutomationRuntimeSnapshot(
            status="waiting_interval",
            message=f"Next cycle is eligible at {next_run_at.isoformat()}",
            connected_workers=connected_workers,
            connected_rss_workers=connected_rss_workers,
            connected_embedding_workers=connected_embedding_workers,
            next_run_at=next_run_at,
            current_ingest_status=current_ingest_status,
            current_embed_status=current_embed_status,
        )

    if connected_rss_workers <= 0:
        return JobAutomationRuntimeSnapshot(
            status="waiting_rss_workers",
            message="Waiting for a connected RSS worker to launch ingest.",
            connected_workers=connected_workers,
            connected_rss_workers=connected_rss_workers,
            connected_embedding_workers=connected_embedding_workers,
            next_run_at=next_run_at,
            current_ingest_status=current_ingest_status,
            current_embed_status=current_embed_status,
        )

    return JobAutomationRuntimeSnapshot(
        status="ready",
        message="Automation is ready to start a new cycle.",
        connected_workers=connected_workers,
        connected_rss_workers=connected_rss_workers,
        connected_embedding_workers=connected_embedding_workers,
        next_run_at=next_run_at,
        current_ingest_status=current_ingest_status,
        current_embed_status=current_embed_status,
    )


def _build_job_automation_read(
    settings: JobAutomationSettingsRecord,
    snapshot: JobAutomationRuntimeSnapshot,
) -> JobAutomationRead:
    return JobAutomationRead(
        enabled=settings.enabled,
        interval_minutes=settings.interval_minutes,
        status=snapshot.status,
        message=snapshot.message,
        connected_workers=snapshot.connected_workers,
        connected_rss_workers=snapshot.connected_rss_workers,
        connected_embedding_workers=snapshot.connected_embedding_workers,
        last_cycle_started_at=settings.last_cycle_started_at,
        next_run_at=snapshot.next_run_at,
        current_ingest_job_id=settings.current_ingest_job_id,
        current_ingest_status=snapshot.current_ingest_status,
        current_embed_job_id=settings.current_embed_job_id,
        current_embed_status=snapshot.current_embed_status,
    )


def _get_or_create_settings(workers_db: Session) -> tuple[JobAutomationSettingsRecord, bool]:
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
            "singleton_key": _SETTINGS_KEY,
            "interval_minutes": _DEFAULT_INTERVAL_MINUTES,
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
            {"singleton_key": _SETTINGS_KEY},
        )
        .mappings()
        .one()
    )
    return _map_settings_record(row), bool(insert_result.rowcount)


def _reconcile_finished_cycle(
    workers_db: Session,
    settings: JobAutomationSettingsRecord,
) -> bool:
    if settings.current_embed_job_id is None:
        return False
    current_embed_status = _get_job_status_value(workers_db, settings.current_embed_job_id)
    if _is_finished_job_status(current_embed_status):
        _update_runtime_fields(
            workers_db,
            last_cycle_started_at=settings.last_cycle_started_at,
            current_ingest_job_id=None,
            current_embed_job_id=None,
        )
        return True
    return False


def _update_runtime_fields(
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
            "singleton_key": _SETTINGS_KEY,
            "last_cycle_started_at": last_cycle_started_at,
            "current_ingest_job_id": current_ingest_job_id,
            "current_embed_job_id": current_embed_job_id,
        },
    )


def _resolve_next_run_at(
    last_cycle_started_at: datetime | None,
    interval_minutes: int,
    *,
    now: datetime,
) -> datetime:
    if last_cycle_started_at is None:
        return now
    normalized_last_cycle_started_at = normalize_datetime_to_utc(last_cycle_started_at)
    return normalized_last_cycle_started_at + timedelta(minutes=max(1, int(interval_minutes)))


def _get_job_status_value(workers_db: Session, job_id: str | None) -> str | None:
    if not job_id:
        return None
    try:
        job = get_job_status(workers_db, job_id=job_id)
    except JobNotFoundError:
        return None
    return str(job.status)


def _map_settings_record(row) -> JobAutomationSettingsRecord:
    return JobAutomationSettingsRecord(
        enabled=bool(row["enabled"]),
        interval_minutes=max(1, int(row["interval_minutes"] or _DEFAULT_INTERVAL_MINUTES)),
        last_cycle_started_at=normalize_datetime_to_utc(row["last_cycle_started_at"]),
        current_ingest_job_id=(
            str(row["current_ingest_job_id"])
            if row["current_ingest_job_id"] is not None
            else None
        ),
        current_embed_job_id=(
            str(row["current_embed_job_id"])
            if row["current_embed_job_id"] is not None
            else None
        ),
    )
def _is_finished_job_status(status: str | None) -> bool:
    return status is None or status in _FINISHED_JOB_STATUSES


def _is_scheduler_disabled() -> bool:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return True
    if "pytest" in sys.modules:
        return True
    raw_value = os.getenv("ADMIN_AUTOMATION_SCHEDULER_ENABLED", "true").strip().lower()
    return raw_value in {"0", "false", "no", "off"}
