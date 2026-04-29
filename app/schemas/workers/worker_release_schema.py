from __future__ import annotations

from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field


WorkerReleaseFamily = Literal["desktop", "rss", "embedding"]
WorkerReleaseDownloadAuth = Literal["public", "worker_bearer"]


class WorkerReleaseCatalogEntrySchema(BaseModel):
    artifact_name: str | None = Field(default=None, min_length=1, max_length=255)
    family: WorkerReleaseFamily | None = None
    product: str = Field(min_length=1, max_length=80)
    platform: str = Field(min_length=1, max_length=40)
    arch: str = Field(min_length=1, max_length=40)
    latest_version: str = Field(min_length=1, max_length=40)
    minimum_supported_version: str = Field(min_length=1, max_length=40)
    worker_version: str | None = Field(default=None, min_length=1, max_length=80)
    artifact_kind: str = Field(min_length=1, max_length=40)
    sha256: str = Field(min_length=1, max_length=128)
    runtime_bundle: str | None = Field(default=None, min_length=1, max_length=40)
    download_auth: WorkerReleaseDownloadAuth | None = None
    download_url: str = Field(min_length=1, max_length=2000)
    release_notes_url: str = Field(min_length=1, max_length=2000)
    published_at: datetime
    storage_relative_path: str | None = Field(default=None, min_length=1, max_length=2000)


class WorkerReleaseCatalogSchema(BaseModel):
    items: list[WorkerReleaseCatalogEntrySchema] = Field(default_factory=list)


class WorkerReleaseManifestRead(BaseModel):
    artifact_name: str = Field(min_length=1, max_length=255)
    family: WorkerReleaseFamily
    product: str = Field(min_length=1, max_length=80)
    platform: str = Field(min_length=1, max_length=40)
    arch: str = Field(min_length=1, max_length=40)
    latest_version: str = Field(min_length=1, max_length=40)
    minimum_supported_version: str = Field(min_length=1, max_length=40)
    worker_version: str | None = Field(default=None, min_length=1, max_length=80)
    artifact_kind: str = Field(min_length=1, max_length=40)
    sha256: str = Field(min_length=1, max_length=128)
    runtime_bundle: str | None = Field(default=None, min_length=1, max_length=40)
    download_auth: WorkerReleaseDownloadAuth
    download_url: str = Field(min_length=1, max_length=2000)
    release_notes_url: str = Field(min_length=1, max_length=2000)
    published_at: datetime


class WorkerDesktopReleaseRead(WorkerReleaseManifestRead):
    title: str = Field(min_length=1, max_length=120)
    platform_label: str = Field(min_length=1, max_length=120)
    download_label: str = Field(min_length=1, max_length=40)
    install_command: str | None = Field(default=None, min_length=1, max_length=255)


class WorkerDesktopReleaseListRead(BaseModel):
    items: list[WorkerDesktopReleaseRead] = Field(default_factory=list)


class WorkerPingRead(BaseModel):
    ok: bool = True
    worker_type: str = Field(min_length=1, max_length=80)
    worker_name: str = Field(min_length=1, max_length=100)
