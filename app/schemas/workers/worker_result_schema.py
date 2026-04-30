from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


WorkerResultStatus = Literal["ok", "error"]


class WorkerResultSourceSchema(BaseModel):
    title: str = Field(min_length=1, max_length=2000)
    summary: str | None = Field(default=None, max_length=20000)
    url: str = Field(min_length=1, max_length=4000)
    published_at: datetime | None = None
    author: str | None = Field(default=None, max_length=255)
    authors: list[str] = Field(default_factory=list)
    image_url: str | None = Field(default=None, max_length=4000)


class WorkerResultSchema(BaseModel):
    feed_id: int = Field(ge=1)
    job_id: str = Field(min_length=1, max_length=128)
    status: WorkerResultStatus
    status_code: int | None = Field(default=None, ge=100, le=599)
    new_etag: str | None = Field(default=None, max_length=1024)
    new_last_update: datetime | None = None
    sources: list[WorkerResultSourceSchema] = Field(default_factory=list)

