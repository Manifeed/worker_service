from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from shared_backend.schemas.internal.worker_service_schema import WorkerServiceStatsRead
from shared_backend.security.internal_service_auth import require_internal_service_token
from app.services.worker_stats_service import read_worker_stats
from database import get_workers_read_db_session


internal_worker_stats_router = APIRouter(
    prefix="/internal/workers",
    tags=["internal-workers"],
    dependencies=[Depends(require_internal_service_token)],
)


@internal_worker_stats_router.get("/stats", response_model=WorkerServiceStatsRead)
def read_internal_worker_stats(
    workers_db: Session = Depends(get_workers_read_db_session),
) -> WorkerServiceStatsRead:
    return read_worker_stats(workers_db)
