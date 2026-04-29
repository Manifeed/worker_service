from pydantic import BaseModel, Field


class InternalWorkerStatsRead(BaseModel):
    connected_workers: int = Field(ge=0)
