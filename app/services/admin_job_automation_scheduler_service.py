from __future__ import annotations

from datetime import datetime, timezone
from threading import Event, Lock, Thread
import logging
import os
import sys

from app.clients.database.admin_job_automation_database_client import (
    JobAutomationSettingsRecord,
    get_or_create_job_automation_settings,
    update_job_automation_runtime_fields,
)
from app.database import open_content_read_db_session, open_workers_write_db_session
from app.domain.job_lock import JobAlreadyRunning, job_lock
from app.services.admin_job_automation_state_service import (
    build_job_automation_runtime_snapshot,
    reconcile_finished_cycle,
)
from app.services.job_enqueue_service import enqueue_rss_scrape_job, enqueue_source_embedding_job
from shared_backend.errors.custom_exceptions import JobAlreadyRunningError


SCHEDULER_POLL_SECONDS = 30
SCHEDULER_GUARD = Lock()
SCHEDULER_THREAD: Thread | None = None
SCHEDULER_STOP_EVENT: Event | None = None
LOGGER = logging.getLogger("app")


def start_admin_job_automation_scheduler() -> None:
    if is_scheduler_disabled():
        return
    global SCHEDULER_THREAD, SCHEDULER_STOP_EVENT
    with SCHEDULER_GUARD:
        if SCHEDULER_THREAD is not None and SCHEDULER_THREAD.is_alive():
            return
        stop_event = Event()
        thread = Thread(
            target=_scheduler_loop,
            args=(stop_event,),
            name="admin-job-automation",
            daemon=True,
        )
        SCHEDULER_STOP_EVENT = stop_event
        SCHEDULER_THREAD = thread
        thread.start()


def stop_admin_job_automation_scheduler() -> None:
    global SCHEDULER_THREAD, SCHEDULER_STOP_EVENT
    with SCHEDULER_GUARD:
        stop_event = SCHEDULER_STOP_EVENT
        thread = SCHEDULER_THREAD
        SCHEDULER_STOP_EVENT = None
        SCHEDULER_THREAD = None
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
            LOGGER.exception("Admin job automation tick failed")
        if stop_event.wait(SCHEDULER_POLL_SECONDS):
            break


def _run_locked_admin_job_automation_tick(content_db, workers_db) -> None:
    settings, initialized = get_or_create_job_automation_settings(workers_db)
    settings_changed = initialized or reconcile_finished_cycle(workers_db, settings)
    if settings_changed:
        workers_db.commit()
        settings, _ = get_or_create_job_automation_settings(workers_db)
    if not settings.enabled:
        return
    snapshot = build_job_automation_runtime_snapshot(workers_db, settings)
    try:
        if snapshot.status == "ready":
            cycle_started_at = datetime.now(timezone.utc)
            enqueue_result = enqueue_rss_scrape_job(content_db, workers_db, commit=False)
            update_job_automation_runtime_fields(
                workers_db,
                last_cycle_started_at=cycle_started_at,
                current_ingest_job_id=enqueue_result.job_id,
                current_embed_job_id=None,
            )
            workers_db.commit()
            LOGGER.info("Admin automation launched ingest job %s", enqueue_result.job_id)
            return
        if snapshot.status == "ready_for_embed":
            enqueue_result = enqueue_source_embedding_job(content_db, workers_db, commit=False)
            update_job_automation_runtime_fields(
                workers_db,
                last_cycle_started_at=settings.last_cycle_started_at,
                current_ingest_job_id=settings.current_ingest_job_id,
                current_embed_job_id=enqueue_result.job_id,
            )
            workers_db.commit()
            LOGGER.info("Admin automation launched embed job %s", enqueue_result.job_id)
            return
    except JobAlreadyRunningError:
        workers_db.rollback()
        return
    except Exception:
        workers_db.rollback()
        raise
    if settings_changed:
        workers_db.commit()


def is_scheduler_disabled() -> bool:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return True
    if "pytest" in sys.modules:
        return True
    raw_value = os.getenv("ADMIN_AUTOMATION_SCHEDULER_ENABLED", "true").strip().lower()
    return raw_value in {"0", "false", "no", "off"}
