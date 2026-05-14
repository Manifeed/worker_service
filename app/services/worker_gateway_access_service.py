from __future__ import annotations

from sqlalchemy.orm import Session

from app.clients.database.worker_gateway_database_client import (
    WorkerLeaseRecord,
    get_worker_lease,
    get_worker_session,
    reserve_worker_lease_result,
)
from app.services.worker_auth_service import AuthenticatedWorkerContext
from shared_backend.errors.custom_exceptions import (
    WorkerLeaseNotFoundError,
    WorkerLeaseStateError,
    WorkerProtocolError,
    WorkerSessionNotFoundError,
)

from app.domain.worker_gateway_signature import utc_now


def require_worker_session(
    db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    session_id: str,
    task_type: str,
    worker_version: str | None,
    expected_worker_type: str,
):
    if worker.worker_type != expected_worker_type:
        raise WorkerProtocolError("Worker token cannot access this task type")
    session = get_worker_session(db, session_id=session_id, api_key_id=worker.api_key_id)
    if session is None:
        raise WorkerSessionNotFoundError("Worker session does not exist")
    if session.expires_at <= utc_now():
        raise WorkerSessionNotFoundError("Worker session has expired")
    if (session.worker_version or None) != (worker_version or None):
        raise WorkerProtocolError("Worker version does not match the opened session")
    return session


def require_worker_lease(
    db: Session,
    *,
    session_id: str,
    lease_id: str,
    task_type: str,
) -> WorkerLeaseRecord:
    lease = get_worker_lease(db, lease_id=lease_id, session_id=session_id)
    if lease is None:
        raise WorkerLeaseNotFoundError("Worker lease does not exist")
    if lease.task_type != task_type:
        raise WorkerProtocolError("Worker lease task type does not match the request")
    return lease


def reserve_or_confirm_lease_finalization(
    db: Session,
    *,
    lease: WorkerLeaseRecord,
    result_status: str,
    result_nonce: str,
    result_signature_hash: str,
) -> bool:
    if lease.result_nonce is not None:
        if (
            lease.result_status == result_status
            and lease.result_nonce == result_nonce
            and lease.result_signature_hash == result_signature_hash
        ):
            return False
        raise WorkerLeaseStateError("Worker lease was already finalized with a different payload")
    if lease.expires_at <= utc_now():
        raise WorkerLeaseStateError(f"Worker lease {lease.lease_id} has expired")
    reserved_lease = reserve_worker_lease_result(
        db,
        lease_id=lease.lease_id,
        session_id=lease.session_id,
        result_status=result_status,
        result_nonce=result_nonce,
        result_signature_hash=result_signature_hash,
    )
    if reserved_lease is not None:
        return True

    current_lease = require_worker_lease(
        db,
        session_id=lease.session_id,
        lease_id=lease.lease_id,
        task_type=lease.task_type,
    )
    if (
        current_lease.result_status == result_status
        and current_lease.result_nonce == result_nonce
        and current_lease.result_signature_hash == result_signature_hash
    ):
        return False
    raise WorkerLeaseStateError("Worker lease was already finalized with a different payload")
