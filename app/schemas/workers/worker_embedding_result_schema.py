from __future__ import annotations

from pydantic import BaseModel, Field


class WorkerSourceEmbeddingSchema(BaseModel):
    id: int = Field(ge=1)
    embedding: list[float] = Field(default_factory=list, min_length=1)


class WorkerEmbeddingResultPayloadSchema(BaseModel):
    sources: list[WorkerSourceEmbeddingSchema] = Field(default_factory=list, min_length=1)
