from __future__ import annotations

from typing import Any

from app.domain.worker_gateway_signature import (
    format_worker_gateway_timestamp,
    verify_worker_gateway_signature,
)
from app.schemas.workers.worker_rss_result_schema import WorkerRssTaskResultPayloadSchema
from app.services.worker_auth_service import AuthenticatedWorkerContext
from shared_backend.errors.custom_exceptions import (
    WorkerProtocolError,
    WorkerSignatureError,
)


def validate_rss_worker_result_payload(payload: dict[str, Any]) -> WorkerRssTaskResultPayloadSchema:
    try:
        return WorkerRssTaskResultPayloadSchema.model_validate(payload)
    except Exception as exception:
        raise WorkerProtocolError(
            f"RSS completion payload does not match the frozen worker contract: {exception}"
        ) from exception


def verify_worker_signature(
    *,
    worker: AuthenticatedWorkerContext,
    payload: dict[str, Any],
    signature: str,
) -> None:
    is_valid = verify_worker_gateway_signature(
        secret=worker.api_key_secret_hash,
        payload=payload,
        signature=signature,
    )
    if not is_valid:
        raise WorkerSignatureError("Worker request signature is invalid")


def resolve_signature_result_payload(
    *,
    task_type: str,
    raw_request_body: bytes | None,
    result_payload: dict[str, Any],
) -> dict[str, Any]:
    del task_type, raw_request_body
    return result_payload


def build_payload_ref(*, task_type: str, task_id: int, execution_id: int) -> str:
    task_namespace = "rss" if is_rss_task_type(task_type) else "unknown"
    return f"{task_namespace}:{task_id}:{execution_id}"


def parse_payload_ref(payload_ref: str) -> tuple[int, int]:
    payload_ref_parts = payload_ref.split(":")
    if len(payload_ref_parts) != 3:
        raise WorkerProtocolError("Worker payload_ref format is invalid")
    try:
        return int(payload_ref_parts[1]), int(payload_ref_parts[2])
    except ValueError as exception:
        raise WorkerProtocolError("Worker payload_ref identifiers are invalid") from exception


def build_lease_signature_payload(
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


def build_result_signature_payload(
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


def build_fail_signature_payload(
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


def resolve_worker_type_for_task(task_type: str) -> str:
    if is_rss_task_type(task_type):
        return "rss_scrapper"
    raise WorkerProtocolError(f"Unsupported worker task type: {task_type}")


def is_rss_task_type(task_type: str) -> bool:
    return task_type.startswith("rss.fetch")
