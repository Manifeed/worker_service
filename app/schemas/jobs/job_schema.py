from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field

from app.schemas.enums import (
    WorkerJobKind,
    WorkerJobStatus,
    WorkerTaskStatus,
)


class JobStatusRead(BaseModel):
    job_id: str
    job_kind: WorkerJobKind
    status: WorkerJobStatus
    requested_at: datetime
    task_total: int = Field(ge=0, default=0)
    task_processed: int = Field(ge=0, default=0)
    item_success: int = Field(ge=0, default=0)
    item_error: int = Field(ge=0, default=0)
    worker_version: str | None = Field(default=None, max_length=80)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    item_total: int = Field(ge=0, default=0)
    finalized_at: datetime | None = None


class JobOverviewItemRead(BaseModel):
    job_id: str
    job_kind: WorkerJobKind
    status: WorkerJobStatus
    requested_at: datetime
    task_total: int = Field(ge=0, default=0)
    task_processed: int = Field(ge=0, default=0)
    item_success: int = Field(ge=0, default=0)
    item_error: int = Field(ge=0, default=0)


class JobsOverviewRead(BaseModel):
    generated_at: datetime
    items: list[JobOverviewItemRead] = Field(default_factory=list)


class JobTaskRead(BaseModel):
    task_id: int = Field(ge=1)
    status: WorkerTaskStatus
    claimed_at: datetime | None = None
    completed_at: datetime | None = None
    claim_expires_at: datetime | None = None
    item_total: int = Field(ge=0, default=0)
    item_success: int = Field(ge=0, default=0)
    item_error: int = Field(ge=0, default=0)
