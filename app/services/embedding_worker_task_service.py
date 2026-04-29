from __future__ import annotations

import math
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from app.errors.custom_exceptions import (
    WorkerProtocolError,
    WorkerTaskStateError,
    WorkerTaskValidationError,
)
from app.clients.networking.qdrant_networking_client import SimpleQdrantClient
from app.schemas.workers.worker_embedding_result_schema import (
    WorkerEmbeddingResultPayloadSchema,
)
from app.clients.database.article_embedding_database_client import get_article_embedding_index_reads
from app.domain.source_embedding_config import (
    FIXED_SOURCE_EMBEDDING_MODEL_NAME,
    resolve_source_embedding_dimensions,
)
from app.clients.database.rss_embedding_finalization_database_client import (
    upsert_embedding_manifest_failed,
    upsert_embedding_manifest_indexed,
)
from app.services.worker_task_finalization_service import (
    complete_claimed_worker_task,
    fail_claimed_worker_task,
    require_claimed_worker_task,
)

def complete_embedding_task(
    content_db: Session,
    workers_db: Session,
    *,
    task_id: int,
    execution_id: int,
    trace_id: str,
    lease_id: str,
    result_payload: dict,
) -> str | None:
    task = require_claimed_worker_task(
        workers_db,
        task_id=task_id,
        execution_id=execution_id,
        task_label="Embedding",
    )
    validated_payload = WorkerEmbeddingResultPayloadSchema.model_validate(result_payload)
    expected_article_ids = {
        int(source['id'])
        for source in task.payload.get('sources', [])
        if isinstance(source, dict) and source.get('id') is not None
    }
    seen_article_ids = {int(source.id) for source in validated_payload.sources}
    if seen_article_ids != expected_article_ids:
        raise WorkerProtocolError(
            "Embedding task completed unexpected article ids: "
            f'expected {sorted(expected_article_ids)}, got {sorted(seen_article_ids)}'
        )
    qdrant_client = SimpleQdrantClient()
    article_index_reads = get_article_embedding_index_reads(content_db, article_ids=sorted(expected_article_ids))
    success_count = 0
    error_count = 0
    for source in validated_payload.sources:
        article_read = article_index_reads.get(int(source.id))
        if article_read is None:
            raise WorkerTaskStateError(f"Missing article metadata for embedding article {source.id}")
        try:
            _validate_embedding_vector(source.embedding)
            qdrant_point_id = qdrant_client.upsert_article_embedding(
                article_id=article_read.article_id,
                article_key=article_read.article_key,
                worker_version=task.worker_version or '',
                vector=source.embedding,
                url=article_read.url,
                title=article_read.title,
                summary=article_read.summary,
                company_id=article_read.company_id,
                company=article_read.company,
                language=article_read.language,
                published_at=article_read.published_at,
                feed_ids=article_read.feed_ids,
                feeds=article_read.feeds,
                author_ids=article_read.author_ids,
                authors=article_read.authors,
                images_url=article_read.images_url,
            )
            upsert_embedding_manifest_indexed(
                content_db,
                article_id=article_read.article_id,
                worker_version=task.worker_version or '',
                model_name=FIXED_SOURCE_EMBEDDING_MODEL_NAME,
                qdrant_point_id=qdrant_point_id,
                indexed_at=datetime.now(timezone.utc),
            )
            success_count += 1
        except WorkerTaskValidationError as exception:
            error_count += 1
            failure_reason = str(exception)
            upsert_embedding_manifest_failed(
                content_db,
                article_id=article_read.article_id,
                worker_version=task.worker_version or '',
                model_name=FIXED_SOURCE_EMBEDDING_MODEL_NAME,
                failure_reason=failure_reason,
            )
    return complete_claimed_worker_task(
        workers_db,
        task=task,
        trace_id=trace_id,
        lease_id=lease_id,
        item_success=success_count,
        item_error=error_count,
        task_label="Embedding",
    )


def fail_embedding_task(
    db: Session,
    *,
    task_id: int,
    execution_id: int,
    trace_id: str,
    lease_id: str,
    error_message: str,
) -> str | None:
    task = require_claimed_worker_task(
        db,
        task_id=task_id,
        execution_id=execution_id,
        task_label="Embedding",
    )
    return fail_claimed_worker_task(
        db,
        task=task,
        trace_id=trace_id,
        lease_id=lease_id,
        error_message=error_message,
        item_error=task.item_total,
        task_label="Embedding",
    )


def _validate_embedding_vector(vector: list[float]) -> tuple[int, float]:
    if not vector:
        raise WorkerTaskValidationError("Embedding vector is empty")
    dimensions = len(vector)
    expected_dimensions = resolve_source_embedding_dimensions()
    if expected_dimensions is not None and dimensions != expected_dimensions:
        raise WorkerTaskValidationError(
            f"Embedding dimensions mismatch: expected {expected_dimensions}, got {dimensions}"
        )
    squared_norm = 0.0
    for value in vector:
        if not math.isfinite(value):
            raise WorkerTaskValidationError("Embedding vector contains non-finite values")
        squared_norm += value * value
    norm = math.sqrt(squared_norm)
    if norm <= 0.0:
        raise WorkerTaskValidationError("Embedding vector norm must be positive")
    return dimensions, norm
