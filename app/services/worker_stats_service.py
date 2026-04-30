from __future__ import annotations

from sqlalchemy.orm import Session

from app.clients.database.worker_gateway_database_client import count_active_worker_sessions
from shared_backend.schemas.internal.worker_service_schema import WorkerServiceStatsRead


def read_worker_stats(workers_db: Session) -> WorkerServiceStatsRead:
    return WorkerServiceStatsRead(
        connected_workers=count_active_worker_sessions(workers_db),
    )
