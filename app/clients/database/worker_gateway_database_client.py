from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared_backend.utils.datetime_utils import normalize_datetime_to_utc

from app.clients.database.worker_gateway_models import (
    WorkerLeaseRecord,
    WorkerSessionRecord,
    map_worker_lease,
    map_worker_session,
)


def create_worker_session(
    db: Session,
    *,
    session_id: str,
    api_key_id: int,
    worker_type: str,
    worker_version: str | None,
    expires_at: datetime,
) -> WorkerSessionRecord:
    row = (
        db.execute(
            text(
                """
                INSERT INTO worker_sessions (
                    session_id,
                    api_key_id,
                    worker_type,
                    worker_version,
                    expires_at
                ) VALUES (
                    :session_id,
                    :api_key_id,
                    :worker_type,
                    :worker_version,
                    :expires_at
                )
                RETURNING
                    session_id,
                    api_key_id,
                    worker_type,
                    worker_version,
                    expires_at
                """
            ),
            {
                "session_id": session_id,
                "api_key_id": api_key_id,
                "worker_type": worker_type,
                "worker_version": worker_version,
                "expires_at": normalize_datetime_to_utc(expires_at),
            },
        )
        .mappings()
        .one()
    )
    return map_worker_session(row)


def get_worker_session(
    db: Session,
    *,
    session_id: str,
    api_key_id: int,
) -> WorkerSessionRecord | None:
    row = (
        db.execute(
            text(
                """
                SELECT
                    session_id,
                    api_key_id,
                    worker_type,
                    worker_version,
                    expires_at
                FROM worker_sessions
                WHERE session_id = :session_id
                    AND api_key_id = :api_key_id
                """
            ),
            {
                "session_id": session_id,
                "api_key_id": api_key_id,
            },
        )
        .mappings()
        .one_or_none()
    )
    return map_worker_session(row) if row is not None else None


def create_worker_lease(
    db: Session,
    *,
    lease_id: str,
    session_id: str,
    task_type: str,
    payload_ref: str,
    expires_at: datetime,
    signature_hash: str,
) -> WorkerLeaseRecord:
    row = (
        db.execute(
            text(
                """
                INSERT INTO worker_leases (
                    lease_id,
                    session_id,
                    task_type,
                    payload_ref,
                    expires_at,
                    result_status,
                    result_nonce,
                    signature_hash,
                    result_signature_hash
                ) VALUES (
                    :lease_id,
                    :session_id,
                    :task_type,
                    :payload_ref,
                    :expires_at,
                    NULL,
                    NULL,
                    :signature_hash,
                    NULL
                )
                RETURNING
                    lease_id,
                    session_id,
                    task_type,
                    payload_ref,
                    expires_at,
                    result_status,
                    result_nonce,
                    signature_hash,
                    result_signature_hash
                """
            ),
            {
                "lease_id": lease_id,
                "session_id": session_id,
                "task_type": task_type,
                "payload_ref": payload_ref,
                "expires_at": normalize_datetime_to_utc(expires_at),
                "signature_hash": signature_hash,
            },
        )
        .mappings()
        .one()
    )
    return map_worker_lease(row)


def get_worker_lease(
    db: Session,
    *,
    lease_id: str,
    session_id: str,
) -> WorkerLeaseRecord | None:
    row = (
        db.execute(
            text(
                """
                SELECT
                    lease_id,
                    session_id,
                    task_type,
                    payload_ref,
                    expires_at,
                    result_status,
                    result_nonce,
                    signature_hash,
                    result_signature_hash
                FROM worker_leases
                WHERE lease_id = :lease_id
                    AND session_id = :session_id
                """
            ),
            {
                "lease_id": lease_id,
                "session_id": session_id,
            },
        )
        .mappings()
        .one_or_none()
    )
    return map_worker_lease(row) if row is not None else None


def reserve_worker_lease_result(
    db: Session,
    *,
    lease_id: str,
    session_id: str,
    result_status: str,
    result_nonce: str,
    result_signature_hash: str,
) -> WorkerLeaseRecord | None:
    row = (
        db.execute(
            text(
                """
                UPDATE worker_leases
                SET
                    result_status = :result_status,
                    result_nonce = :result_nonce,
                    result_signature_hash = :result_signature_hash
                WHERE lease_id = :lease_id
                    AND session_id = :session_id
                    AND result_nonce IS NULL
                RETURNING
                    lease_id,
                    session_id,
                    task_type,
                    payload_ref,
                    expires_at,
                    result_status,
                    result_nonce,
                    signature_hash,
                    result_signature_hash
                """
            ),
            {
                "lease_id": lease_id,
                "session_id": session_id,
                "result_status": result_status,
                "result_nonce": result_nonce,
                "result_signature_hash": result_signature_hash,
            },
        )
        .mappings()
        .one_or_none()
    )
    return map_worker_lease(row) if row is not None else None


def count_active_worker_sessions(db: Session, *, worker_type: str | None = None) -> int:
    filters = ["expires_at >= now()"]
    params: dict[str, object] = {}
    if worker_type:
        filters.append("worker_type = :worker_type")
        params["worker_type"] = worker_type
    return int(
        db.execute(
            text(
                """
                SELECT COUNT(*)
                FROM worker_sessions
                WHERE """
                + " AND ".join(filters)
            ),
            params,
        ).scalar_one()
        or 0
    )
