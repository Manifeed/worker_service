from __future__ import annotations

from fastapi import APIRouter, Depends

from shared_backend.schemas.workers.worker_release_schema import WorkerPingRead
from app.services.worker_release_service import read_worker_ping
from app.services.worker_auth_service import (
    AuthenticatedWorkerContext,
    require_authenticated_worker_context,
)


worker_release_router = APIRouter(prefix="/workers/api", tags=["workers-release"])


@worker_release_router.get("/ping", response_model=WorkerPingRead)
def read_authenticated_worker_ping(
    worker: AuthenticatedWorkerContext = Depends(require_authenticated_worker_context),
) -> WorkerPingRead:
    return read_worker_ping(worker=worker)
