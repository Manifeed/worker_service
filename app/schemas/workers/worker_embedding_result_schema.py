from __future__ import annotations

from pydantic import BaseModel, Field


class WorkerEmbeddingSourceSchema(BaseModel):
    id: int = Field(ge=1)
    embedding: list[float] = Field(default_factory=list)


class WorkerEmbeddingResultPayloadSchema(BaseModel):
    sources: list[WorkerEmbeddingSourceSchema] = Field(default_factory=list)
