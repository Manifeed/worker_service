from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field


class RssScrapeFeedPayloadSchema(BaseModel):
    feed_id: int = Field(ge=1)
    feed_url: str = Field(min_length=1, max_length=500)
    company_id: int | None = Field(default=None, ge=1)
    host_header: str | None = Field(default=None, min_length=1, max_length=255)
    fetchprotection: int = Field(default=1, ge=0, le=2)
    etag: str | None = Field(default=None, max_length=255)
    last_update: datetime | None = None
    last_db_article_published_at: datetime | None = None


class RssScrapeJobRequestSchema(BaseModel):
    job_id: str = Field(min_length=1, max_length=64)
    requested_at: datetime
    ingest: bool
    feeds: list[RssScrapeFeedPayloadSchema] = Field(default_factory=list)
