from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.clients.database.admin_job_automation_database_client import (
    JobAutomationSettingsRecord,
    update_job_automation_runtime_fields,
)
from app.clients.database.worker_gateway_database_client import count_active_worker_sessions
from app.clients.database.worker_job_database_client import get_active_worker_job_id
from app.services.job_read_service import get_job_status
from shared_backend.errors.custom_exceptions import JobNotFoundError
from shared_backend.schemas.enums import WorkerJobKind, WorkerKind
from shared_backend.schemas.jobs.job_automation_schema import JobAutomationRead
from shared_backend.utils.datetime_utils import normalize_datetime_to_utc


FINISHED_JOB_STATUSES = {"cancelled", "completed", "completed_with_errors", "failed"}


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


def build_job_automation_runtime_snapshot(
    workers_db: Session,
    settings: JobAutomationSettingsRecord,
) -> JobAutomationRuntimeSnapshot:
    now = datetime.now(timezone.utc)
    connected_rss_workers = count_active_worker_sessions(
        workers_db,
        worker_type=WorkerKind.RSS_SCRAPPER.value,
    )
    connected_embedding_workers = 0
    connected_workers = connected_rss_workers
    current_ingest_status = get_job_status_value(workers_db, settings.current_ingest_job_id)
    current_embed_status = get_job_status_value(workers_db, settings.current_embed_job_id)
    active_rss_job_id = get_active_worker_job_id(
        workers_db,
        job_kind=WorkerJobKind.RSS_SCRAPE.value,
    )
    active_embedding_job_id = get_active_worker_job_id(
        workers_db,
        job_kind=WorkerJobKind.SOURCE_EMBEDDING.value,
    )
    next_run_at = (
        resolve_next_run_at(settings.last_cycle_started_at, settings.interval_minutes, now=now)
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
    if settings.current_embed_job_id and not is_finished_job_status(current_embed_status):
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
        if not is_finished_job_status(current_ingest_status):
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


def build_job_automation_read(
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


def reconcile_finished_cycle(
    workers_db: Session,
    settings: JobAutomationSettingsRecord,
) -> bool:
    if settings.current_embed_job_id is None:
        return False
    current_embed_status = get_job_status_value(workers_db, settings.current_embed_job_id)
    if is_finished_job_status(current_embed_status):
        update_job_automation_runtime_fields(
            workers_db,
            last_cycle_started_at=settings.last_cycle_started_at,
            current_ingest_job_id=None,
            current_embed_job_id=None,
        )
        return True
    return False


def resolve_next_run_at(
    last_cycle_started_at: datetime | None,
    interval_minutes: int,
    *,
    now: datetime,
) -> datetime:
    if last_cycle_started_at is None:
        return now
    normalized_last_cycle_started_at = normalize_datetime_to_utc(last_cycle_started_at)
    return normalized_last_cycle_started_at + timedelta(minutes=max(1, int(interval_minutes)))


def get_job_status_value(workers_db: Session, job_id: str | None) -> str | None:
    if not job_id:
        return None
    try:
        job = get_job_status(workers_db, job_id=job_id)
    except JobNotFoundError:
        return None
    return str(job.status)


def is_finished_job_status(status: str | None) -> bool:
    return status is None or status in FINISHED_JOB_STATUSES
