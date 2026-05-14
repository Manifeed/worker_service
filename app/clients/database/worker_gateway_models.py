from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from shared_backend.utils.datetime_utils import normalize_datetime_to_utc


@dataclass(frozen=True)
class WorkerSessionRecord:
    session_id: str
    api_key_id: int
    worker_type: str
    worker_version: str | None
    expires_at: datetime


@dataclass(frozen=True)
class WorkerLeaseRecord:
    lease_id: str
    session_id: str
    task_type: str
    payload_ref: str
    expires_at: datetime
    result_status: str | None
    result_nonce: str | None
    signature_hash: str
    result_signature_hash: str | None


def map_worker_session(row) -> WorkerSessionRecord:
    return WorkerSessionRecord(
        session_id=str(row["session_id"]),
        api_key_id=int(row["api_key_id"]),
        worker_type=str(row["worker_type"]),
        worker_version=str(row["worker_version"]) if row["worker_version"] is not None else None,
        expires_at=normalize_datetime_to_utc(row["expires_at"]) or datetime.now(timezone.utc),
    )


def map_worker_lease(row) -> WorkerLeaseRecord:
    return WorkerLeaseRecord(
        lease_id=str(row["lease_id"]),
        session_id=str(row["session_id"]),
        task_type=str(row["task_type"]),
        payload_ref=str(row["payload_ref"]),
        expires_at=normalize_datetime_to_utc(row["expires_at"]) or datetime.now(timezone.utc),
        result_status=str(row["result_status"]) if row["result_status"] is not None else None,
        result_nonce=str(row["result_nonce"]) if row["result_nonce"] is not None else None,
        signature_hash=str(row["signature_hash"]),
        result_signature_hash=(
            str(row["result_signature_hash"])
            if row["result_signature_hash"] is not None
            else None
        ),
    )
