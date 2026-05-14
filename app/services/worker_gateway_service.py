from __future__ import annotations

from datetime import timedelta

from sqlalchemy.orm import Session

from app.clients.database.auth_database_client import (
    touch_user_api_key_last_used,
    upsert_api_key_worker_usage,
)
from app.clients.database.worker_gateway_database_client import create_worker_session
from app.domain.worker_gateway_signature import (
    generate_worker_gateway_id,
    hash_worker_gateway_signature,
    utc_now,
)
from app.schemas.workers.worker_gateway_schema import (
    WorkerLeaseRead,
    WorkerSessionOpenRead,
    WorkerSessionOpenRequestSchema,
    WorkerTaskClaimRequestSchema,
    WorkerTaskCompleteRequestSchema,
    WorkerTaskFailRequestSchema,
)
from app.services.worker_auth_service import AuthenticatedWorkerContext
from app.services.worker_gateway_access_service import (
    require_worker_lease,
    require_worker_session,
    reserve_or_confirm_lease_finalization,
)
from app.services.worker_gateway_lease_service import build_worker_lease_read
from app.services.worker_gateway_signature_service import (
    build_fail_signature_payload,
    build_result_signature_payload,
    parse_payload_ref,
    resolve_signature_result_payload,
    resolve_worker_type_for_task,
    verify_worker_signature,
)
from app.services.worker_gateway_task_service import (
    claim_tasks,
    complete_task,
    fail_task,
)
from shared_backend.errors.custom_exceptions import WorkerProtocolError


def open_worker_session(
    identity_db: Session,
    workers_db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    payload: WorkerSessionOpenRequestSchema,
) -> WorkerSessionOpenRead:
    expected_worker_type = resolve_worker_type_for_task(payload.task_type)
    _require_worker_type(worker, expected_worker_type)
    session_id = generate_worker_gateway_id("ws")
    expires_at = utc_now() + timedelta(seconds=payload.session_ttl_seconds)
    create_worker_session(
        workers_db,
        session_id=session_id,
        api_key_id=worker.api_key_id,
        worker_type=worker.worker_type,
        worker_version=payload.worker_version,
        expires_at=expires_at,
    )
    touch_user_api_key_last_used(identity_db, api_key_id=worker.api_key_id)
    upsert_api_key_worker_usage(
        identity_db,
        api_key_id=worker.api_key_id,
        worker_name=worker.worker_name,
        worker_type=worker.worker_type,
        worker_version=payload.worker_version,
    )
    try:
        workers_db.commit()
        identity_db.commit()
    except Exception:
        workers_db.rollback()
        identity_db.rollback()
        raise
    return WorkerSessionOpenRead(
        session_id=session_id,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
        expires_at=expires_at,
    )


def claim_worker_session_tasks(
    content_db: Session,
    workers_db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    payload: WorkerTaskClaimRequestSchema,
) -> list[WorkerLeaseRead]:
    session = _require_active_session(
        workers_db,
        worker=worker,
        session_id=payload.session_id,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
    )
    claimed_tasks = claim_tasks(content_db, workers_db, worker=worker, payload=payload)
    lease_reads: list[WorkerLeaseRead] = []
    for task in claimed_tasks:
        lease_reads.append(
            build_worker_lease_read(
                workers_db,
                session_id=session.session_id,
                task=task,
                task_type=payload.task_type,
                worker_version=payload.worker_version,
                worker_secret=worker.api_key_secret_hash,
                lease_seconds=payload.lease_seconds,
            )
        )
    workers_db.commit()
    return lease_reads


def complete_worker_session_task(
    content_db: Session,
    workers_db: Session | None = None,
    *,
    worker: AuthenticatedWorkerContext,
    payload: WorkerTaskCompleteRequestSchema,
    raw_request_body: bytes | None = None,
) -> str | None:
    single_db_session = workers_db is None
    workers_db = workers_db or content_db
    session = _require_active_session(
        workers_db,
        worker=worker,
        session_id=payload.session_id,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
    )
    lease = require_worker_lease(
        workers_db,
        session_id=session.session_id,
        lease_id=payload.lease_id,
        task_type=payload.task_type,
    )
    signature_result_payload = resolve_signature_result_payload(
        task_type=payload.task_type,
        raw_request_body=raw_request_body,
        result_payload=payload.result_payload,
    )
    signature_payload = build_result_signature_payload(
        session_id=payload.session_id,
        lease_id=payload.lease_id,
        trace_id=payload.trace_id,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
        signed_at=payload.signed_at,
        nonce=payload.nonce,
        result_payload=signature_result_payload,
    )
    verify_worker_signature(worker=worker, payload=signature_payload, signature=payload.signature)
    finalization = reserve_or_confirm_lease_finalization(
        workers_db,
        lease=lease,
        result_status="completed",
        result_nonce=payload.nonce,
        result_signature_hash=hash_worker_gateway_signature(payload.signature),
    )
    if not finalization:
        workers_db.rollback()
        return None
    task_id, execution_id = parse_payload_ref(lease.payload_ref)
    try:
        job_id_to_finalize = complete_task(
            content_db,
            workers_db,
            worker=worker,
            payload=payload,
            task_id=task_id,
            execution_id=execution_id,
        )
    except Exception:
        content_db.rollback()
        if not single_db_session:
            workers_db.rollback()
        raise
    if single_db_session:
        content_db.commit()
    else:
        content_db.commit()
        workers_db.commit()
    return job_id_to_finalize


def fail_worker_session_task(
    workers_db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    payload: WorkerTaskFailRequestSchema,
) -> str | None:
    session = _require_active_session(
        workers_db,
        worker=worker,
        session_id=payload.session_id,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
    )
    lease = require_worker_lease(
        workers_db,
        session_id=session.session_id,
        lease_id=payload.lease_id,
        task_type=payload.task_type,
    )
    signature_payload = build_fail_signature_payload(
        session_id=payload.session_id,
        lease_id=payload.lease_id,
        trace_id=payload.trace_id,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
        signed_at=payload.signed_at,
        nonce=payload.nonce,
        error_message=payload.error_message,
    )
    verify_worker_signature(worker=worker, payload=signature_payload, signature=payload.signature)
    finalization = reserve_or_confirm_lease_finalization(
        workers_db,
        lease=lease,
        result_status="failed",
        result_nonce=payload.nonce,
        result_signature_hash=hash_worker_gateway_signature(payload.signature),
    )
    if not finalization:
        workers_db.rollback()
        return None
    task_id, execution_id = parse_payload_ref(lease.payload_ref)
    try:
        job_id_to_finalize = fail_task(
            workers_db,
            worker=worker,
            payload=payload,
            task_id=task_id,
            execution_id=execution_id,
        )
    except Exception:
        workers_db.rollback()
        raise
    workers_db.commit()
    return job_id_to_finalize


def _require_active_session(
    db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    session_id: str,
    task_type: str,
    worker_version: str | None,
):
    expected_worker_type = resolve_worker_type_for_task(task_type)
    return require_worker_session(
        db,
        worker=worker,
        session_id=session_id,
        task_type=task_type,
        worker_version=worker_version,
        expected_worker_type=expected_worker_type,
    )


def _require_worker_type(worker: AuthenticatedWorkerContext, expected_worker_type: str) -> None:
    if worker.worker_type != expected_worker_type:
        raise WorkerProtocolError("Worker token cannot access this task type")
