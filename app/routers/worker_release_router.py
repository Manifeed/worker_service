from pathlib import Path
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import FileResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from shared_backend.errors.custom_exceptions import WorkerReleaseNotFoundError
from shared_backend.schemas.workers.worker_release_schema import (
    WorkerDesktopReleaseListRead,
    WorkerPingRead,
    WorkerReleaseManifestRead,
)
from app.services.worker_release_service import (
    authorize_worker_release_download,
    list_worker_desktop_releases,
    read_worker_ping,
    read_worker_release_download_entry,
    read_worker_release_manifest,
    resolve_worker_release_storage_path,
)
from app.services.worker_auth_service import (
    AuthenticatedWorkerContext,
    require_authenticated_worker_context,
)
from database import get_identity_db_session


worker_release_router = APIRouter(prefix="/workers/api", tags=["workers-release"])
_worker_download_bearer_scheme = HTTPBearer(auto_error=False)


def _request_base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _resolve_download_path(artifact_name: str, download_url: str) -> str:
    parsed = urlparse(download_url)
    if parsed.path.startswith("/workers/api/releases/download/"):
        return parsed.path
    return f"/workers/api/releases/download/{artifact_name}"


def _rewrite_release_urls(
    payload: WorkerReleaseManifestRead | WorkerDesktopReleaseListRead,
    request: Request,
) -> WorkerReleaseManifestRead | WorkerDesktopReleaseListRead:
    base_url = _request_base_url(request)

    if isinstance(payload, WorkerDesktopReleaseListRead):
        return payload.model_copy(
            update={
                "items": [
                    item.model_copy(
                        update={
                            "download_url": (
                                f"{base_url}"
                                f"{_resolve_download_path(item.artifact_name, item.download_url)}"
                            ),
                            "release_notes_url": f"{base_url}/workers",
                        }
                    )
                    for item in payload.items
                ]
            }
        )

    return payload.model_copy(
        update={
            "download_url": (
                f"{base_url}"
                f"{_resolve_download_path(payload.artifact_name, payload.download_url)}"
            ),
            "release_notes_url": f"{base_url}/workers",
        }
    )


@worker_release_router.get("/ping", response_model=WorkerPingRead)
def read_authenticated_worker_ping(
    worker: AuthenticatedWorkerContext = Depends(require_authenticated_worker_context),
) -> WorkerPingRead:
    return read_worker_ping(worker=worker)


@worker_release_router.get("/releases/manifest", response_model=WorkerReleaseManifestRead)
def read_release_manifest(
    request: Request,
    product: str = Query(min_length=1, max_length=80),
    platform: str = Query(min_length=1, max_length=40),
    arch: str = Query(min_length=1, max_length=40),
    runtime_bundle: str | None = Query(default=None, min_length=1, max_length=40),
) -> WorkerReleaseManifestRead:
    manifest = read_worker_release_manifest(
        product=product, platform=platform, arch=arch, runtime_bundle=runtime_bundle
    )
    return _rewrite_release_urls(manifest, request)


@worker_release_router.get("/releases/desktop", response_model=WorkerDesktopReleaseListRead)
def list_public_desktop_releases(request: Request) -> WorkerDesktopReleaseListRead:
    return _rewrite_release_urls(list_worker_desktop_releases(), request)


@worker_release_router.get("/releases/download/{artifact_name}")
def download_release_artifact(
    request: Request,
    artifact_name: str,
    credentials: HTTPAuthorizationCredentials | None = Depends(_worker_download_bearer_scheme),
    db: Session = Depends(get_identity_db_session),
) -> FileResponse:
    entry = read_worker_release_download_entry(artifact_name=artifact_name)
    if entry.download_auth == "worker_bearer":
        worker = require_authenticated_worker_context(request=request, credentials=credentials, db=db)
        authorize_worker_release_download(entry=entry, worker=worker)

    artifact_path = resolve_worker_release_storage_path(entry)
    if not Path(artifact_path).is_file():
        raise WorkerReleaseNotFoundError(
            f"Release artifact file is missing for artifact_name={entry.artifact_name}"
        )
    return FileResponse(
        path=artifact_path,
        filename=entry.artifact_name,
        media_type="application/octet-stream",
    )
