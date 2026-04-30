from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class WorkerSessionOpenRequestSchema(BaseModel):
    task_type: str = Field(min_length=1, max_length=64)
    worker_version: str | None = Field(default=None, min_length=1, max_length=80)
    session_ttl_seconds: int = Field(ge=30, le=86400)


class WorkerSessionOpenRead(BaseModel):
    session_id: str = Field(min_length=1, max_length=64)
    task_type: str = Field(min_length=1, max_length=64)
    worker_version: str | None = Field(default=None, min_length=1, max_length=80)
    expires_at: datetime


class WorkerTaskClaimRequestSchema(BaseModel):
    session_id: str = Field(min_length=1, max_length=64)
    task_type: str = Field(min_length=1, max_length=64)
    worker_version: str | None = Field(default=None, min_length=1, max_length=80)
    count: int = Field(ge=1, le=100)
    lease_seconds: int = Field(ge=30, le=86400)


class WorkerLeaseRead(BaseModel):
    lease_id: str = Field(min_length=1, max_length=64)
    trace_id: str = Field(min_length=1, max_length=64)
    task_type: str = Field(min_length=1, max_length=64)
    worker_version: str | None = Field(default=None, min_length=1, max_length=80)
    task_id: int = Field(ge=1)
    execution_id: int = Field(ge=1)
    payload_ref: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any] = Field(default_factory=dict)
    expires_at: datetime
    signed_at: datetime
    nonce: str = Field(min_length=1, max_length=128)
    signature: str = Field(min_length=1, max_length=256)


class WorkerTaskCompleteRequestSchema(BaseModel):
    session_id: str = Field(min_length=1, max_length=64)
    lease_id: str = Field(min_length=1, max_length=64)
    trace_id: str = Field(min_length=1, max_length=64)
    task_type: str = Field(min_length=1, max_length=64)
    worker_version: str | None = Field(default=None, min_length=1, max_length=80)
    signed_at: datetime
    nonce: str = Field(min_length=1, max_length=128)
    signature: str = Field(min_length=1, max_length=256)
    result_payload: dict[str, Any] = Field(default_factory=dict)


class WorkerTaskFailRequestSchema(BaseModel):
    session_id: str = Field(min_length=1, max_length=64)
    lease_id: str = Field(min_length=1, max_length=64)
    trace_id: str = Field(min_length=1, max_length=64)
    task_type: str = Field(min_length=1, max_length=64)
    worker_version: str | None = Field(default=None, min_length=1, max_length=80)
    signed_at: datetime
    nonce: str = Field(min_length=1, max_length=128)
    signature: str = Field(min_length=1, max_length=256)
    error_message: str = Field(min_length=1, max_length=2000)


class WorkerTaskCommandRead(BaseModel):
    ok: bool = True

