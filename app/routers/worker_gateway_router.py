from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from app.schemas.workers.worker_gateway_schema import (
    WorkerLeaseRead,
    WorkerSessionOpenRead,
    WorkerSessionOpenRequestSchema,
    WorkerTaskClaimRequestSchema,
    WorkerTaskCommandRead,
    WorkerTaskCompleteRequestSchema,
    WorkerTaskFailRequestSchema,
)
from app.services.worker_gateway_service import (
    AuthenticatedWorkerContext,
    claim_worker_session_tasks,
    complete_worker_session_task,
    fail_worker_session_task,
    open_worker_session,
)
from app.services.worker_auth_service import (
    require_authenticated_worker_context,
)

from database import (
    get_content_write_db_session,
    get_identity_read_db_session,
    get_workers_write_db_session,
)

worker_gateway_router = APIRouter(prefix="/workers/api", tags=["workers"])


@worker_gateway_router.post("/sessions/open", response_model=WorkerSessionOpenRead)
def open_session_for_worker(
    payload: WorkerSessionOpenRequestSchema,
    worker: AuthenticatedWorkerContext = Depends(require_authenticated_worker_context),
    identity_db: Session = Depends(get_identity_read_db_session),
    workers_db: Session = Depends(get_workers_write_db_session),
) -> WorkerSessionOpenRead:
    return open_worker_session(identity_db, workers_db, worker=worker, payload=payload)


@worker_gateway_router.post("/tasks/claim", response_model=list[WorkerLeaseRead])
def claim_tasks_for_worker(
    payload: WorkerTaskClaimRequestSchema,
    worker: AuthenticatedWorkerContext = Depends(require_authenticated_worker_context),
    workers_db: Session = Depends(get_workers_write_db_session),
) -> list[WorkerLeaseRead]:
    return claim_worker_session_tasks(workers_db, worker=worker, payload=payload)


@worker_gateway_router.post("/tasks/complete", response_model=WorkerTaskCommandRead)
async def complete_task_for_worker(
    request: Request,
    payload: WorkerTaskCompleteRequestSchema,
    worker: AuthenticatedWorkerContext = Depends(require_authenticated_worker_context),
    content_db: Session = Depends(get_content_write_db_session),
    workers_db: Session = Depends(get_workers_write_db_session),
) -> WorkerTaskCommandRead:
    raw_request_body = await request.body()
    complete_worker_session_task(
        content_db,
        workers_db,
        worker=worker,
        payload=payload,
        raw_request_body=raw_request_body,
    )
    return WorkerTaskCommandRead(ok=True)


@worker_gateway_router.post("/tasks/fail", response_model=WorkerTaskCommandRead)
def fail_task_for_worker(
    payload: WorkerTaskFailRequestSchema,
    worker: AuthenticatedWorkerContext = Depends(require_authenticated_worker_context),
    workers_db: Session = Depends(get_workers_write_db_session),
) -> WorkerTaskCommandRead:
    fail_worker_session_task(workers_db, worker=worker, payload=payload)
    return WorkerTaskCommandRead(ok=True)
