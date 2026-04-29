from __future__ import annotations

from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session


def upsert_embedding_manifest_indexed(
    db: Session,
    *,
    article_id: int,
    worker_version: str,
    model_name: str,
    qdrant_point_id: str,
    indexed_at: datetime,
) -> None:
    db.execute(
        text(
            """
            INSERT INTO embedding_manifest (
                article_id,
                worker_version,
                model_name,
                status,
                qdrant_point_id,
                indexed_at,
                failure_reason
            ) VALUES (
                :article_id,
                :worker_version,
                :model_name,
                'indexed',
                :qdrant_point_id,
                :indexed_at,
                NULL
            )
            ON CONFLICT (article_id, worker_version) DO UPDATE SET
                model_name = EXCLUDED.model_name,
                status = 'indexed',
                qdrant_point_id = EXCLUDED.qdrant_point_id,
                indexed_at = EXCLUDED.indexed_at,
                failure_reason = NULL
            """
        ),
        {
            "article_id": article_id,
            "worker_version": worker_version,
            "model_name": model_name,
            "qdrant_point_id": qdrant_point_id,
            "indexed_at": indexed_at,
        },
    )


def upsert_embedding_manifest_failed(
    db: Session,
    *,
    article_id: int,
    worker_version: str,
    model_name: str,
    failure_reason: str,
) -> None:
    db.execute(
        text(
            """
            INSERT INTO embedding_manifest (
                article_id,
                worker_version,
                model_name,
                status,
                qdrant_point_id,
                indexed_at,
                failure_reason
            ) VALUES (
                :article_id,
                :worker_version,
                :model_name,
                'failed',
                NULL,
                NULL,
                :failure_reason
            )
            ON CONFLICT (article_id, worker_version) DO UPDATE SET
                model_name = EXCLUDED.model_name,
                status = 'failed',
                qdrant_point_id = NULL,
                indexed_at = NULL,
                failure_reason = EXCLUDED.failure_reason
            """
        ),
        {
            "article_id": article_id,
            "worker_version": worker_version,
            "model_name": model_name,
            "failure_reason": failure_reason,
        },
    )
