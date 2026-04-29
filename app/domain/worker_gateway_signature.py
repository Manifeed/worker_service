from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any


class CanonicalJsonNumber(str):
    pass


def generate_worker_gateway_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(16)}"


def generate_worker_gateway_nonce() -> str:
    return secrets.token_hex(16)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_worker_gateway_timestamp(value: datetime) -> str:
    normalized_value = value.astimezone(timezone.utc).replace(microsecond=0)
    return normalized_value.isoformat().replace("+00:00", "Z")


def hash_worker_gateway_signature(signature: str) -> str:
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def canonicalize_worker_gateway_payload(payload: Mapping[str, Any]) -> str:
    return _serialize_worker_gateway_payload(_normalize_worker_gateway_payload(payload))


def sign_worker_gateway_payload(*, secret: str, payload: Mapping[str, Any]) -> str:
    canonical_payload = canonicalize_worker_gateway_payload(payload)
    digest = hmac.new(secret.encode("utf-8"), canonical_payload.encode("utf-8"), hashlib.sha256)
    return digest.hexdigest()


def verify_worker_gateway_signature(
    *,
    secret: str,
    payload: Mapping[str, Any],
    signature: str,
) -> bool:
    expected_signature = sign_worker_gateway_payload(secret=secret, payload=payload)
    return hmac.compare_digest(expected_signature, signature)


def _normalize_worker_gateway_payload(value: Any) -> Any:
    if isinstance(value, datetime):
        return format_worker_gateway_timestamp(value)
    if isinstance(value, Mapping):
        return {
            str(key): _normalize_worker_gateway_payload(item_value)
            for key, item_value in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, list):
        return [_normalize_worker_gateway_payload(item_value) for item_value in value]
    if isinstance(value, tuple):
        return [_normalize_worker_gateway_payload(item_value) for item_value in value]
    return value


def _serialize_worker_gateway_payload(value: Any) -> str:
    if isinstance(value, CanonicalJsonNumber):
        return str(value)
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Mapping):
        serialized_entries = [
            f"{json.dumps(str(key), ensure_ascii=True, separators=(',', ':'))}:{_serialize_worker_gateway_payload(item_value)}"
            for key, item_value in sorted(value.items(), key=lambda item: str(item[0]))
        ]
        return f"{{{','.join(serialized_entries)}}}"
    if isinstance(value, list):
        return f"[{','.join(_serialize_worker_gateway_payload(item_value) for item_value in value)}]"
    if isinstance(value, tuple):
        return f"[{','.join(_serialize_worker_gateway_payload(item_value) for item_value in value)}]"
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), allow_nan=False)
