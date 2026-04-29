from __future__ import annotations

from pydantic import BaseModel, Field


class RssSourceEmbeddingPayloadSchema(BaseModel):
    id: int = Field(ge=1)
    title: str = Field(min_length=1, max_length=500)
    summary: str | None = None
    url: str = Field(min_length=1, max_length=1000)


class RssSourceEmbeddingRequestSchema(BaseModel):
    sources: list[RssSourceEmbeddingPayloadSchema] = Field(default_factory=list, min_length=1)
