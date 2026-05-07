from __future__ import annotations

from dataclasses import dataclass
import json
from datetime import timedelta
from typing import Any
from sqlalchemy.orm import Session

from app.clients.database.auth_database_client import (
    touch_user_api_key_last_used,
    upsert_api_key_worker_usage,
)
from shared_backend.errors.custom_exceptions import (
    WorkerLeaseNotFoundError,
    WorkerLeaseStateError,
    WorkerProtocolError,
    WorkerSessionNotFoundError,
    WorkerSignatureError,
)
from app.schemas.workers.worker_gateway_schema import (
    WorkerLeaseRead,
    WorkerSessionOpenRead,
    WorkerSessionOpenRequestSchema,
    WorkerTaskClaimRequestSchema,
    WorkerTaskCompleteRequestSchema,
    WorkerTaskFailRequestSchema,
)
from app.schemas.workers.worker_rss_result_schema import WorkerRssTaskResultPayloadSchema
from app.domain.worker_gateway_signature import (
    CanonicalJsonNumber,
    format_worker_gateway_timestamp,
    generate_worker_gateway_id,
    generate_worker_gateway_nonce,
    hash_worker_gateway_signature,
    sign_worker_gateway_payload,
    utc_now,
    verify_worker_gateway_signature,
)
from app.clients.database.worker_job_database_client import (
    claim_worker_tasks as claim_worker_task_rows,
)
from app.clients.database.worker_gateway_database_client import (
    WorkerLeaseRecord,
    create_worker_lease,
    create_worker_session,
    get_worker_lease,
    get_worker_session,
    reserve_worker_lease_result,
)
from app.services.rss_worker_task_service import (
    complete_rss_task,
    fail_rss_task,
)
from app.services.worker_auth_service import AuthenticatedWorkerContext


@dataclass(frozen=True)
class ClaimedTaskRead:
    task_id: int
    execution_id: int
    payload: dict[str, Any]


def open_worker_session(
    identity_db: Session,
    workers_db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    payload: WorkerSessionOpenRequestSchema,
) -> WorkerSessionOpenRead:
    expected_worker_type = _resolve_worker_type_for_task(payload.task_type)
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
    workers_db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    payload: WorkerTaskClaimRequestSchema,
) -> list[WorkerLeaseRead]:
    session = _require_worker_session(
        workers_db,
        worker=worker,
        session_id=payload.session_id,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
    )
    claimed_tasks = _claim_tasks(workers_db, payload=payload)
    lease_reads: list[WorkerLeaseRead] = []
    for task in claimed_tasks:
        lease_id = generate_worker_gateway_id("lease")
        trace_id = generate_worker_gateway_id("trace")
        nonce = generate_worker_gateway_nonce()
        signed_at = utc_now()
        expires_at = signed_at + timedelta(seconds=payload.lease_seconds)
        payload_ref = _build_payload_ref(
            task_type=payload.task_type,
            task_id=task.task_id,
            execution_id=task.execution_id,
        )
        lease_signature_payload = _build_lease_signature_payload(
            lease_id=lease_id,
            trace_id=trace_id,
            task_type=payload.task_type,
            worker_version=payload.worker_version,
            task_id=task.task_id,
            execution_id=task.execution_id,
            payload_ref=payload_ref,
            payload=task.payload,
            expires_at=expires_at,
            signed_at=signed_at,
            nonce=nonce,
        )
        signature = sign_worker_gateway_payload(
            secret=worker.api_key_secret_hash,
            payload=lease_signature_payload,
        )
        create_worker_lease(
            workers_db,
            lease_id=lease_id,
            session_id=session.session_id,
            task_type=payload.task_type,
            payload_ref=payload_ref,
            expires_at=expires_at,
            signature_hash=hash_worker_gateway_signature(signature),
        )
        lease_reads.append(
            WorkerLeaseRead(
                lease_id=lease_id,
                trace_id=trace_id,
                task_type=payload.task_type,
                worker_version=payload.worker_version,
                task_id=task.task_id,
                execution_id=task.execution_id,
                payload_ref=payload_ref,
                payload=task.payload,
                expires_at=expires_at,
                signed_at=signed_at,
                nonce=nonce,
                signature=signature,
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
    session = _require_worker_session(
        workers_db,
        worker=worker,
        session_id=payload.session_id,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
    )
    lease = _require_worker_lease(
        workers_db,
        session_id=session.session_id,
        lease_id=payload.lease_id,
        task_type=payload.task_type,
    )
    signature_result_payload = _resolve_signature_result_payload(
        task_type=payload.task_type,
        raw_request_body=raw_request_body,
        result_payload=payload.result_payload,
    )
    signature_payload = _build_result_signature_payload(
        session_id=payload.session_id,
        lease_id=payload.lease_id,
        trace_id=payload.trace_id,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
        signed_at=payload.signed_at,
        nonce=payload.nonce,
        result_payload=signature_result_payload,
    )
    _verify_worker_signature(worker=worker, payload=signature_payload, signature=payload.signature)
    result_signature_hash = hash_worker_gateway_signature(payload.signature)
    finalization = _reserve_or_confirm_lease_finalization(
        workers_db,
        lease=lease,
        result_status="completed",
        result_nonce=payload.nonce,
        result_signature_hash=result_signature_hash,
    )
    if not finalization:
        workers_db.rollback()
        return None
    task_id, execution_id = _parse_payload_ref(lease.payload_ref)
    try:
        job_id_to_finalize = _complete_task(
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
    session = _require_worker_session(
        workers_db,
        worker=worker,
        session_id=payload.session_id,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
    )
    lease = _require_worker_lease(
        workers_db,
        session_id=session.session_id,
        lease_id=payload.lease_id,
        task_type=payload.task_type,
    )
    signature_payload = _build_fail_signature_payload(
        session_id=payload.session_id,
        lease_id=payload.lease_id,
        trace_id=payload.trace_id,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
        signed_at=payload.signed_at,
        nonce=payload.nonce,
        error_message=payload.error_message,
    )
    _verify_worker_signature(worker=worker, payload=signature_payload, signature=payload.signature)
    result_signature_hash = hash_worker_gateway_signature(payload.signature)
    finalization = _reserve_or_confirm_lease_finalization(
        workers_db,
        lease=lease,
        result_status="failed",
        result_nonce=payload.nonce,
        result_signature_hash=result_signature_hash,
    )
    if not finalization:
        workers_db.rollback()
        return None
    task_id, execution_id = _parse_payload_ref(lease.payload_ref)
    try:
        job_id_to_finalize = _fail_task(
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


def _claim_tasks(db: Session, *, payload: WorkerTaskClaimRequestSchema) -> list[ClaimedTaskRead]:
    claimed_rows = claim_worker_task_rows(
        db,
        task_type=payload.task_type,
        worker_version=payload.worker_version,
        task_count=payload.count,
        lease_seconds=payload.lease_seconds,
    )
    return [
        ClaimedTaskRead(
            task_id=row.task_id,
            execution_id=row.execution_id,
            payload=row.payload,
        )
        for row in claimed_rows
    ]


def _complete_task(
    content_db: Session,
    workers_db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    payload: WorkerTaskCompleteRequestSchema,
    task_id: int,
    execution_id: int,
) -> str | None:
    if _is_rss_task_type(payload.task_type):
        rss_result_payload = _validate_rss_worker_result_payload(payload.result_payload)
        return complete_rss_task(
            content_db,
            workers_db,
            worker_name=worker.worker_name,
            task_id=task_id,
            execution_id=execution_id,
            trace_id=payload.trace_id,
            lease_id=payload.lease_id,
            result_payload=rss_result_payload,
        )
    raise WorkerProtocolError(f"Unsupported worker task type: {payload.task_type}")


def _fail_task(
    db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    payload: WorkerTaskFailRequestSchema,
    task_id: int,
    execution_id: int,
) -> str | None:
    if _is_rss_task_type(payload.task_type):
        return fail_rss_task(
            db,
            task_id=task_id,
            execution_id=execution_id,
            trace_id=payload.trace_id,
            lease_id=payload.lease_id,
            error_message=payload.error_message,
        )
    raise WorkerProtocolError(f"Unsupported worker task type: {payload.task_type}")


def _validate_rss_worker_result_payload(payload: dict[str, Any]) -> WorkerRssTaskResultPayloadSchema:
    try:
        return WorkerRssTaskResultPayloadSchema.model_validate(payload)
    except Exception as exception:
        raise WorkerProtocolError(
            f"RSS completion payload does not match the frozen worker contract: {exception}"
        ) from exception


def _require_worker_session(
    db: Session,
    *,
    worker: AuthenticatedWorkerContext,
    session_id: str,
    task_type: str,
    worker_version: str | None,
):
    expected_worker_type = _resolve_worker_type_for_task(task_type)
    _require_worker_type(worker, expected_worker_type)
    session = get_worker_session(db, session_id=session_id, api_key_id=worker.api_key_id)
    if session is None:
        raise WorkerSessionNotFoundError("Worker session does not exist")
    if session.expires_at <= utc_now():
        raise WorkerSessionNotFoundError("Worker session has expired")
    if (session.worker_version or None) != (worker_version or None):
        raise WorkerProtocolError("Worker version does not match the opened session")
    return session


def _require_worker_lease(db: Session, *, session_id: str, lease_id: str, task_type: str):
    lease = get_worker_lease(db, lease_id=lease_id, session_id=session_id)
    if lease is None:
        raise WorkerLeaseNotFoundError("Worker lease does not exist")
    if lease.task_type != task_type:
        raise WorkerProtocolError("Worker lease task type does not match the request")
    return lease


def _reserve_or_confirm_lease_finalization(
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

    current_lease = _require_worker_lease(
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


def _verify_worker_signature(*, worker: AuthenticatedWorkerContext, payload: dict[str, Any], signature: str) -> None:
    is_valid = verify_worker_gateway_signature(
        secret=worker.api_key_secret_hash,
        payload=payload,
        signature=signature,
    )
    if not is_valid:
        raise WorkerSignatureError("Worker request signature is invalid")


def _resolve_signature_result_payload(
    *,
    task_type: str,
    raw_request_body: bytes | None,
    result_payload: dict[str, Any],
) -> dict[str, Any]:
    if not task_type.startswith("embed") or not raw_request_body:
        return result_payload
    try:
        raw_payload = json.loads(
            raw_request_body.decode("utf-8"),
            parse_float=CanonicalJsonNumber,
            parse_int=CanonicalJsonNumber,
        )
    except (UnicodeDecodeError, json.JSONDecodeError):
        return result_payload
    raw_result_payload = raw_payload.get("result_payload")
    if isinstance(raw_result_payload, dict):
        return raw_result_payload
    return result_payload


def _resolve_worker_type_for_task(task_type: str) -> str:
    if _is_rss_task_type(task_type):
        return "rss_scrapper"
    raise WorkerProtocolError(f"Unsupported worker task type: {task_type}")


def _is_rss_task_type(task_type: str) -> bool:
    return task_type.startswith("rss.fetch")


def _require_worker_type(worker: AuthenticatedWorkerContext, expected_worker_type: str) -> None:
    if worker.worker_type != expected_worker_type:
        raise WorkerProtocolError("Worker token cannot access this task type")


def _build_payload_ref(*, task_type: str, task_id: int, execution_id: int) -> str:
    task_namespace = "rss" if _is_rss_task_type(task_type) else "unknown"
    return f"{task_namespace}:{task_id}:{execution_id}"


def _parse_payload_ref(payload_ref: str) -> tuple[int, int]:
    payload_ref_parts = payload_ref.split(":")
    if len(payload_ref_parts) != 3:
        raise WorkerProtocolError("Worker payload_ref format is invalid")
    try:
        return int(payload_ref_parts[1]), int(payload_ref_parts[2])
    except ValueError as exception:
        raise WorkerProtocolError("Worker payload_ref identifiers are invalid") from exception


def _build_lease_signature_payload(
    *,
    lease_id: str,
    trace_id: str,
    task_type: str,
    worker_version: str | None,
    task_id: int,
    execution_id: int,
    payload_ref: str,
    payload: dict[str, Any],
    expires_at,
    signed_at,
    nonce: str,
) -> dict[str, Any]:
    return {
        "lease_id": lease_id,
        "trace_id": trace_id,
        "task_type": task_type,
        "worker_version": worker_version,
        "task_id": task_id,
        "execution_id": execution_id,
        "payload_ref": payload_ref,
        "payload": payload,
        "expires_at": format_worker_gateway_timestamp(expires_at),
        "signed_at": format_worker_gateway_timestamp(signed_at),
        "nonce": nonce,
    }


def _build_result_signature_payload(
    *,
    session_id: str,
    lease_id: str,
    trace_id: str,
    task_type: str,
    worker_version: str | None,
    signed_at,
    nonce: str,
    result_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "lease_id": lease_id,
        "trace_id": trace_id,
        "task_type": task_type,
        "worker_version": worker_version,
        "signed_at": format_worker_gateway_timestamp(signed_at),
        "nonce": nonce,
        "result_payload": result_payload,
    }


def _build_fail_signature_payload(
    *,
    session_id: str,
    lease_id: str,
    trace_id: str,
    task_type: str,
    worker_version: str | None,
    signed_at,
    nonce: str,
    error_message: str,
) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "lease_id": lease_id,
        "trace_id": trace_id,
        "task_type": task_type,
        "worker_version": worker_version,
        "signed_at": format_worker_gateway_timestamp(signed_at),
        "nonce": nonce,
        "error_message": error_message,
    }
