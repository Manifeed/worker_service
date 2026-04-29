from __future__ import annotations

from typing import Literal
from pydantic import BaseModel, Field, model_validator

from .worker_result_schema import WorkerResultSchema


class WorkerRssLocalDedupGroupSchema(BaseModel):
    dedup_key: str = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=64)
    kept_url: str | None = Field(default=None, min_length=1, max_length=1000)
    dropped_urls: list[str] = Field(default_factory=list)


class WorkerRssTaskLocalDedupSchema(BaseModel):
    scope: Literal["task"] = "task"
    input_candidates: int = Field(ge=0)
    output_candidates: int = Field(ge=0)
    duplicates_dropped: int = Field(ge=0)
    groups: list[WorkerRssLocalDedupGroupSchema] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_counts(self) -> "WorkerRssTaskLocalDedupSchema":
        if self.output_candidates > self.input_candidates:
            raise ValueError("output_candidates cannot be greater than input_candidates")
        if self.duplicates_dropped != self.input_candidates - self.output_candidates:
            raise ValueError("duplicates_dropped must match input_candidates - output_candidates")
        return self


class WorkerRssTaskResultPayloadSchema(BaseModel):
    contract_version: Literal["rss-worker-result"] = "rss-worker-result"
    result_events: list[WorkerResultSchema] = Field(default_factory=list)
    local_dedup: WorkerRssTaskLocalDedupSchema

    @model_validator(mode="after")
    def validate_local_dedup_consistency(self) -> "WorkerRssTaskResultPayloadSchema":
        kept_candidates = 0
        for result_event in self.result_events:
            kept_candidates += len(result_event.sources)
        if self.local_dedup.output_candidates != kept_candidates:
            raise ValueError("local_dedup.output_candidates must match returned candidate count")
        return self
