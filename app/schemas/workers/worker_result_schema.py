from __future__ import annotations

from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field, model_validator


class WorkerSourceSchema(BaseModel):
    title: str = Field(min_length=1)
    url: str = Field(min_length=1, max_length=1000)
    summary: str | None = None
    authors: list[str] = Field(default_factory=list)
    author: str | None = None
    published_at: datetime | None = None
    image_url: str | None = None

    @model_validator(mode="after")
    def ensure_authors_collection(self) -> "WorkerSourceSchema":
        if not self.authors and self.author:
            self.authors = [self.author]
        return self


WorkerResultStatus = Literal["success", "not_modified", "error"]


class WorkerResultSchema(BaseModel):
    job_id: str
    ingest: bool
    feed_id: int = Field(ge=1)
    feed_url: str = Field(min_length=1, max_length=500)
    status: WorkerResultStatus
    status_code: int | None = None
    error_message: str | None = None
    new_etag: str | None = None
    new_last_update: datetime | None = None
    fetchprotection: int = Field(ge=0, le=2)
    resolved_fetchprotection: int | None = Field(default=None, ge=0, le=2)
    sources: list[WorkerSourceSchema] = Field(default_factory=list)
