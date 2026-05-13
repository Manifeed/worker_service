from __future__ import annotations

from shared_backend.schemas.workers.worker_release_schema import WorkerPingRead
from app.services.worker_auth_service import AuthenticatedWorkerContext


def read_worker_ping(
    *,
    worker: AuthenticatedWorkerContext,
) -> WorkerPingRead:
    return WorkerPingRead(
        ok=True,
        worker_type=worker.worker_type,
        worker_name=worker.worker_name,
    )
