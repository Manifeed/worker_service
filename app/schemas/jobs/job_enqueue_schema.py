from __future__ import annotations

from pydantic import BaseModel, Field, PositiveInt

from app.schemas.enums import WorkerJobKind, WorkerJobStatus


class RssScrapeJobCreateRequestSchema(BaseModel):
    feed_ids: list[PositiveInt] = Field(default_factory=list)


class SourceEmbeddingJobCreateRequestSchema(BaseModel):
    reembed_model_mismatches: bool = False


class JobEnqueueRead(BaseModel):
    job_id: str
    job_kind: WorkerJobKind
    status: WorkerJobStatus
    worker_version: str | None = Field(default=None, max_length=80)
    tasks_total: int = Field(ge=0, default=0)
    items_total: int = Field(ge=0, default=0)
