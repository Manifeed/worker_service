from __future__ import annotations

from pydantic import BaseModel, Field

from app.schemas.workers.worker_result_schema import WorkerResultSchema


class WorkerRssTaskLocalDedupSchema(BaseModel):
    article_keys: list[str] = Field(default_factory=list)
    content_keys: list[str] = Field(default_factory=list)


class WorkerRssTaskResultPayloadSchema(BaseModel):
    contract_version: str = Field(min_length=1, max_length=80)
    result_events: list[WorkerResultSchema] = Field(default_factory=list)
    local_dedup: WorkerRssTaskLocalDedupSchema = Field(
        default_factory=WorkerRssTaskLocalDedupSchema
    )

