from __future__ import annotations

import json
import os
from typing import Any

import redis

from app.domain.source_embedding_config import resolve_embedding_redis_queue_name


class RedisEmbeddingQueueError(RuntimeError):
    """Raised when the embedding queue cannot be written."""


class RedisEmbeddingQueueClient:
    def __init__(self) -> None:
        redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0").strip() or "redis://redis:6379/0"
        self.queue_name = resolve_embedding_redis_queue_name()
        self._client = redis.Redis.from_url(redis_url, decode_responses=True)

    def enqueue_embedding_messages(self, payloads: list[dict[str, Any]]) -> int:
        if not payloads:
            return 0
        try:
            encoded_payloads = [
                json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
                for payload in payloads
            ]
            return int(self._client.rpush(self.queue_name, *encoded_payloads))
        except redis.RedisError as exception:
            raise RedisEmbeddingQueueError("Unable to enqueue embedding Redis messages") from exception
