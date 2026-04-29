from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class JobAutomationUpdateRequestSchema(BaseModel):
    enabled: bool


class JobAutomationRead(BaseModel):
    enabled: bool
    interval_minutes: int = Field(ge=1, le=1440)
    status: str = Field(min_length=1, max_length=64)
    message: str = Field(min_length=1, max_length=512)
    connected_workers: int = Field(ge=0)
    connected_rss_workers: int = Field(ge=0)
    connected_embedding_workers: int = Field(ge=0)
    last_cycle_started_at: datetime | None = None
    next_run_at: datetime | None = None
    current_ingest_job_id: str | None = Field(default=None, max_length=128)
    current_ingest_status: str | None = Field(default=None, max_length=64)
    current_embed_job_id: str | None = Field(default=None, max_length=128)
    current_embed_status: str | None = Field(default=None, max_length=64)
