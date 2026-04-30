from __future__ import annotations

from dataclasses import dataclass
from time import monotonic
import os

from app.clients.networking.redis_networking_client import (
    RedisCommandError,
    RedisNetworkingClient,
)
from fastapi import Request

from shared_backend.errors.custom_exceptions import RateLimitExceededError
from app.utils.environment_utils import is_production_like_environment


@dataclass
class _MemoryBucket:
    count: int
    expires_at: float


_memory_buckets: dict[str, _MemoryBucket] = {}


def enforce_rate_limit(
    request: Request,
    *,
    namespace: str,
    limit: int,
    window_seconds: int,
    identifier: str | None = None,
) -> None:
    if not _rate_limit_enabled():
        return

    key = _build_rate_limit_key(
        namespace=namespace,
        identifier=identifier or _client_identifier(request),
    )
    count = _increment_redis_bucket(key, window_seconds)
    if count is None:
        if _redis_required_for_rate_limit():
            raise RateLimitExceededError("Rate limiting is temporarily unavailable")
        count = _increment_memory_bucket(key, window_seconds)
    if count > limit:
        raise RateLimitExceededError()


def _rate_limit_enabled() -> bool:
    raw_value = os.getenv("RATE_LIMIT_ENABLED", "true")
    return raw_value.strip().lower() not in {"0", "false", "no", "off"}


def _redis_required_for_rate_limit() -> bool:
    raw_value = os.getenv("RATE_LIMIT_REDIS_REQUIRED")
    if raw_value is not None:
        return raw_value.strip().lower() in {"1", "true", "yes", "on"}
    return is_production_like_environment()


def _build_rate_limit_key(*, namespace: str, identifier: str) -> str:
    safe_identifier = identifier.strip().lower() or "unknown"
    return f"manifeed:rate-limit:{namespace}:{safe_identifier}"


def _client_identifier(request: Request) -> str:
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _increment_memory_bucket(key: str, window_seconds: int) -> int:
    now = monotonic()
    expired_keys = [
        bucket_key
        for bucket_key, bucket in _memory_buckets.items()
        if bucket.expires_at <= now
    ]
    for bucket_key in expired_keys:
        _memory_buckets.pop(bucket_key, None)

    bucket = _memory_buckets.get(key)
    if bucket is None or bucket.expires_at <= now:
        bucket = _MemoryBucket(count=0, expires_at=now + window_seconds)
        _memory_buckets[key] = bucket
    bucket.count += 1
    return bucket.count


def _increment_redis_bucket(key: str, window_seconds: int) -> int | None:
    try:
        return RedisNetworkingClient().increment_with_ttl(key, window_seconds)
    except RedisCommandError:
        return None
