from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class InternalServiceHealthRead(BaseModel):
    service: str = Field(min_length=1, max_length=80)
    status: str = Field(min_length=1, max_length=32)


class InternalResolvedSessionRead(BaseModel):
    user_id: int = Field(ge=1)
    email: str
    role: str
    is_active: bool
    api_access_enabled: bool
    session_expires_at: datetime
