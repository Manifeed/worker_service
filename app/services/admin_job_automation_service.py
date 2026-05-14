from __future__ import annotations

from sqlalchemy.orm import Session

from app.clients.database.admin_job_automation_database_client import (
    get_or_create_job_automation_settings,
    update_job_automation_enabled,
)
from app.services.admin_job_automation_scheduler_service import (
    run_admin_job_automation_tick,
    start_admin_job_automation_scheduler,
    stop_admin_job_automation_scheduler,
)
from app.services.admin_job_automation_state_service import (
    build_job_automation_read,
    build_job_automation_runtime_snapshot,
    reconcile_finished_cycle,
)
from shared_backend.schemas.jobs.job_automation_schema import (
    JobAutomationRead,
    JobAutomationUpdateRequestSchema,
)


def read_job_automation(workers_db: Session) -> JobAutomationRead:
    settings, initialized = get_or_create_job_automation_settings(workers_db)
    if initialized:
        workers_db.commit()
        settings, _ = get_or_create_job_automation_settings(workers_db)
    if reconcile_finished_cycle(workers_db, settings):
        workers_db.commit()
        settings, _ = get_or_create_job_automation_settings(workers_db)
    snapshot = build_job_automation_runtime_snapshot(workers_db, settings)
    return build_job_automation_read(settings, snapshot)


def update_job_automation(
    workers_db: Session,
    payload: JobAutomationUpdateRequestSchema,
) -> JobAutomationRead:
    _, initialized = get_or_create_job_automation_settings(workers_db)
    if initialized:
        workers_db.commit()
    settings = update_job_automation_enabled(workers_db, enabled=payload.enabled)
    workers_db.commit()
    snapshot = build_job_automation_runtime_snapshot(workers_db, settings)
    return build_job_automation_read(settings, snapshot)


__all__ = [
    "read_job_automation",
    "run_admin_job_automation_tick",
    "start_admin_job_automation_scheduler",
    "stop_admin_job_automation_scheduler",
    "update_job_automation",
]
