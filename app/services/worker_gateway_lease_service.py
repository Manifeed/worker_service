from __future__ import annotations

from datetime import timedelta

from sqlalchemy.orm import Session

from app.clients.database.worker_gateway_database_client import create_worker_lease
from app.domain.worker_gateway_signature import (
    generate_worker_gateway_id,
    generate_worker_gateway_nonce,
    hash_worker_gateway_signature,
    sign_worker_gateway_payload,
    utc_now,
)
from app.schemas.workers.worker_gateway_schema import WorkerLeaseRead
from app.services.worker_gateway_signature_service import (
    build_lease_signature_payload,
    build_payload_ref,
)
from app.services.worker_gateway_task_service import ClaimedTaskRead


def build_worker_lease_read(
    workers_db: Session,
    *,
    session_id: str,
    task: ClaimedTaskRead,
    task_type: str,
    worker_version: str | None,
    worker_secret: str,
    lease_seconds: int,
) -> WorkerLeaseRead:
    lease_id = generate_worker_gateway_id("lease")
    trace_id = generate_worker_gateway_id("trace")
    nonce = generate_worker_gateway_nonce()
    signed_at = utc_now()
    expires_at = signed_at + timedelta(seconds=lease_seconds)
    payload_ref = build_payload_ref(
        task_type=task_type,
        task_id=task.task_id,
        execution_id=task.execution_id,
    )
    lease_signature_payload = build_lease_signature_payload(
        lease_id=lease_id,
        trace_id=trace_id,
        task_type=task_type,
        worker_version=worker_version,
        task_id=task.task_id,
        execution_id=task.execution_id,
        payload_ref=payload_ref,
        payload=task.payload,
        expires_at=expires_at,
        signed_at=signed_at,
        nonce=nonce,
    )
    signature = sign_worker_gateway_payload(
        secret=worker_secret,
        payload=lease_signature_payload,
    )
    create_worker_lease(
        workers_db,
        lease_id=lease_id,
        session_id=session_id,
        task_type=task_type,
        payload_ref=payload_ref,
        expires_at=expires_at,
        signature_hash=hash_worker_gateway_signature(signature),
    )
    return WorkerLeaseRead(
        lease_id=lease_id,
        trace_id=trace_id,
        task_type=task_type,
        worker_version=worker_version,
        task_id=task.task_id,
        execution_id=task.execution_id,
        payload_ref=payload_ref,
        payload=task.payload,
        expires_at=expires_at,
        signed_at=signed_at,
        nonce=nonce,
        signature=signature,
    )
