from __future__ import annotations

from sqlalchemy.orm import Session

from app.clients.database.worker_gateway_database_client import count_active_worker_sessions
from app.schemas.internal.worker_stats_schema import InternalWorkerStatsRead


def read_worker_stats(workers_db: Session) -> InternalWorkerStatsRead:
    return InternalWorkerStatsRead(
        connected_workers=count_active_worker_sessions(workers_db),
    )
