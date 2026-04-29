from fastapi import APIRouter, Body, Depends, Path, Query
from sqlalchemy.orm import Session

from app.security import require_internal_service_token
from app.schemas.jobs.job_enqueue_schema import (
    JobEnqueueRead,
    RssScrapeJobCreateRequestSchema,
    SourceEmbeddingJobCreateRequestSchema,
)
from app.schemas.jobs.job_automation_schema import (
    JobAutomationRead,
    JobAutomationUpdateRequestSchema,
)
from app.schemas.jobs.job_schema import JobStatusRead, JobTaskRead, JobsOverviewRead
from app.services.admin_job_automation_service import (
    read_job_automation,
    update_job_automation,
)
from app.services.job_enqueue_service import (
    enqueue_rss_scrape_job,
    enqueue_source_embedding_job,
)
from app.services.job_read_service import (
    get_job_status,
    list_job_tasks,
    list_jobs,
)

from database import get_content_db_session, get_workers_db_session

internal_jobs_router = APIRouter(
    prefix="/internal/jobs",
    tags=["internal-jobs"],
    dependencies=[Depends(require_internal_service_token)],
)


@internal_jobs_router.get("", response_model=JobsOverviewRead)
def read_jobs_overview(
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_workers_db_session),
) -> JobsOverviewRead:
    return list_jobs(db, limit=limit)


@internal_jobs_router.post("/rss-scrape", response_model=JobEnqueueRead)
def create_rss_scrape_job(
    payload: RssScrapeJobCreateRequestSchema | None = Body(default=None),
    content_db: Session = Depends(get_content_db_session),
    workers_db: Session = Depends(get_workers_db_session),
) -> JobEnqueueRead:
    normalized_payload = payload or RssScrapeJobCreateRequestSchema()
    return enqueue_rss_scrape_job(
        content_db,
        workers_db,
        feed_ids=normalized_payload.feed_ids or None,
    )


@internal_jobs_router.post("/source-embedding", response_model=JobEnqueueRead)
def create_source_embedding_job(
    payload: SourceEmbeddingJobCreateRequestSchema | None = Body(default=None),
    content_db: Session = Depends(get_content_db_session),
    workers_db: Session = Depends(get_workers_db_session),
) -> JobEnqueueRead:
    normalized_payload = payload or SourceEmbeddingJobCreateRequestSchema()
    return enqueue_source_embedding_job(
        content_db,
        workers_db,
        reembed_model_mismatches=normalized_payload.reembed_model_mismatches,
    )


@internal_jobs_router.get("/automation", response_model=JobAutomationRead)
def read_job_automation_route(
    workers_db: Session = Depends(get_workers_db_session),
) -> JobAutomationRead:
    return read_job_automation(workers_db)


@internal_jobs_router.patch("/automation", response_model=JobAutomationRead)
def update_job_automation_route(
    payload: JobAutomationUpdateRequestSchema,
    workers_db: Session = Depends(get_workers_db_session),
) -> JobAutomationRead:
    return update_job_automation(workers_db, payload)


@internal_jobs_router.get("/{job_id}/tasks", response_model=list[JobTaskRead])
def read_job_tasks(
    job_id: str = Path(min_length=1),
    db: Session = Depends(get_workers_db_session),
) -> list[JobTaskRead]:
    return list_job_tasks(db, job_id=job_id)


@internal_jobs_router.get("/{job_id}", response_model=JobStatusRead)
def read_job_status(
    job_id: str = Path(min_length=1),
    db: Session = Depends(get_workers_db_session),
) -> JobStatusRead:
    return get_job_status(db, job_id=job_id)
