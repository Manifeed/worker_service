from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

from shared_backend.errors.custom_exceptions import (
    WorkerReleaseCatalogError,
    WorkerReleaseDownloadForbiddenError,
    WorkerReleaseNotFoundError,
)
from shared_backend.schemas.workers.worker_release_schema import (
    WorkerDesktopReleaseListRead,
    WorkerDesktopReleaseRead,
    WorkerPingRead,
    WorkerReleaseCatalogEntrySchema,
    WorkerReleaseCatalogSchema,
    WorkerReleaseManifestRead,
)
from app.domain.worker_release_policy import (
    build_desktop_release_presentation,
    desktop_release_sort_key,
    resolve_worker_release_product_policy,
)
from app.services.worker_auth_service import AuthenticatedWorkerContext


WORKER_RELEASE_CATALOG_PATH_ENV = "WORKER_RELEASE_CATALOG_PATH"
WORKER_RELEASE_CATALOG_JSON_ENV = "WORKER_RELEASE_CATALOG_JSON"
WORKER_RELEASE_MANIFEST_PATH_ENV = "WORKER_RELEASE_MANIFEST_PATH"
WORKER_RELEASE_MANIFEST_JSON_ENV = "WORKER_RELEASE_MANIFEST_JSON"
WORKER_RELEASE_STORAGE_ROOT_ENV = "WORKER_RELEASE_STORAGE_ROOT"
_SEMVER_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$")
_ARTIFACT_VERSION_PATTERN = re.compile(r"(\d+\.\d+\.\d+)")


def read_worker_release_manifest(
    *,
    product: str,
    platform: str,
    arch: str,
    runtime_bundle: str | None = None,
) -> WorkerReleaseManifestRead:
    normalized_platform = _normalize_platform(platform)
    normalized_arch = _normalize_arch(arch)
    normalized_runtime_bundle = _normalize_runtime_bundle(runtime_bundle)
    for item in _iter_latest_catalog_items():
        if (
            item.product == product
            and _normalize_platform(item.platform) == normalized_platform
            and _normalize_arch(item.arch) == normalized_arch
            and _normalize_runtime_bundle(item.runtime_bundle) == normalized_runtime_bundle
        ):
            return _build_manifest_read(item)
    raise WorkerReleaseNotFoundError(
        "No worker release manifest configured for "
        f"product={product}, platform={normalized_platform}, arch={normalized_arch}, "
        f"runtime_bundle={normalized_runtime_bundle or 'none'}"
    )


def list_worker_desktop_releases() -> WorkerDesktopReleaseListRead:
    items: list[WorkerDesktopReleaseRead] = []
    for entry in sorted(
        (
            item
            for item in _iter_latest_catalog_items()
            if item.family == "desktop" and item.download_auth == "public"
        ),
        key=lambda item: desktop_release_sort_key(
            _normalize_platform(item.platform),
            _normalize_arch(item.arch),
        ),
    ):
        title, platform_label, download_label, install_command = (
            build_desktop_release_presentation(
                platform=_normalize_platform(entry.platform),
                arch=_normalize_arch(entry.arch),
                artifact_name=_require_value(entry.artifact_name, "artifact_name"),
            )
        )
        items.append(
            WorkerDesktopReleaseRead(
                **_build_manifest_read(entry).model_dump(),
                title=title,
                platform_label=platform_label,
                download_label=download_label,
                install_command=install_command,
            )
        )
    return WorkerDesktopReleaseListRead(items=items)


def read_worker_release_download_entry(
    *,
    artifact_name: str,
) -> WorkerReleaseCatalogEntrySchema:
    normalized_artifact_name = artifact_name.strip()
    if not normalized_artifact_name:
        raise WorkerReleaseNotFoundError("Release artifact name is required")

    for item in _iter_normalized_catalog_items():
        if item.artifact_name == normalized_artifact_name:
            return item

    raise WorkerReleaseNotFoundError(
        f"No worker release artifact configured for artifact_name={normalized_artifact_name}"
    )


def authorize_worker_release_download(
    *,
    entry: WorkerReleaseCatalogEntrySchema,
    worker: AuthenticatedWorkerContext,
) -> None:
    required_worker_type = resolve_worker_release_product_policy(entry.product).required_worker_type
    if required_worker_type is None:
        return
    if worker.worker_type != required_worker_type:
        raise WorkerReleaseDownloadForbiddenError(
            f"Worker API key for {worker.worker_type} cannot download {entry.product}"
        )


def resolve_worker_release_storage_path(entry: WorkerReleaseCatalogEntrySchema) -> Path:
    storage_root = _resolve_storage_root()
    storage_relative_path = _require_value(
        entry.storage_relative_path,
        "storage_relative_path",
    )
    resolved_path = storage_root.joinpath(storage_relative_path).resolve()
    try:
        resolved_path.relative_to(storage_root)
    except ValueError as exc:
        raise WorkerReleaseCatalogError(
            f"Release artifact path escapes storage root: {storage_relative_path}"
        ) from exc
    return resolved_path


def resolve_active_rss_worker_version() -> str | None:
    return resolve_active_worker_family_version("rss")


def resolve_active_embedding_worker_version() -> str | None:
    return resolve_active_worker_family_version("embedding")


def resolve_active_worker_family_version(family: str) -> str | None:
    versions = {
        _resolved_worker_version(item)
        for item in _iter_latest_catalog_items()
        if item.family == family
    }
    versions.discard("")
    if not versions:
        return None
    if len(versions) > 1:
        raise WorkerReleaseCatalogError(
            f"Worker release family {family} has conflicting worker versions: {sorted(versions)}"
        )
    return next(iter(versions))


def read_worker_ping(
    *,
    worker: AuthenticatedWorkerContext,
) -> WorkerPingRead:
    return WorkerPingRead(
        ok=True,
        worker_type=worker.worker_type,
        worker_name=worker.worker_name,
    )


def clear_worker_release_manifest_cache() -> None:
    clear_worker_release_catalog_cache()


def clear_worker_release_catalog_cache() -> None:
    _load_worker_release_catalog_cached.cache_clear()


def _load_worker_release_catalog() -> WorkerReleaseCatalogSchema:
    raw_json = (
        os.getenv(WORKER_RELEASE_CATALOG_JSON_ENV, "").strip()
        or os.getenv(WORKER_RELEASE_MANIFEST_JSON_ENV, "").strip()
    )
    path_value = (
        os.getenv(WORKER_RELEASE_CATALOG_PATH_ENV, "").strip()
        or os.getenv(WORKER_RELEASE_MANIFEST_PATH_ENV, "").strip()
    )
    catalog_path, catalog_mtime_ns = _catalog_path_cache_key(path_value)
    return _load_worker_release_catalog_cached(raw_json, catalog_path, catalog_mtime_ns)


@lru_cache(maxsize=8)
def _load_worker_release_catalog_cached(
    raw_json: str,
    path_value: str | None,
    path_mtime_ns: int | None,
) -> WorkerReleaseCatalogSchema:
    if raw_json:
        return WorkerReleaseCatalogSchema.model_validate(json.loads(raw_json))
    if path_value:
        path = Path(path_value)
        if not path.exists():
            return WorkerReleaseCatalogSchema(items=[])
        payload = path.read_text(encoding="utf-8")
        return WorkerReleaseCatalogSchema.model_validate(json.loads(payload))
    return WorkerReleaseCatalogSchema(items=[])


def _catalog_path_cache_key(path_value: str) -> tuple[str | None, int | None]:
    normalized = path_value.strip()
    if not normalized:
        return (None, None)

    path = Path(normalized)
    try:
        stat = path.stat()
    except FileNotFoundError:
        return (str(path), None)

    return (str(path), stat.st_mtime_ns)


def _iter_normalized_catalog_items() -> list[WorkerReleaseCatalogEntrySchema]:
    return [_normalize_catalog_item(item) for item in _load_worker_release_catalog().items]


def _iter_latest_catalog_items() -> list[WorkerReleaseCatalogEntrySchema]:
    latest_by_key: dict[tuple[str, str, str, str, str | None], WorkerReleaseCatalogEntrySchema] = {}
    for item in _iter_normalized_catalog_items():
        key = _catalog_release_key(item)
        current = latest_by_key.get(key)
        if current is None or _catalog_item_rank(item) > _catalog_item_rank(current):
            latest_by_key[key] = item
    return list(latest_by_key.values())


def _normalize_catalog_item(
    item: WorkerReleaseCatalogEntrySchema,
) -> WorkerReleaseCatalogEntrySchema:
    policy = resolve_worker_release_product_policy(item.product)
    artifact_name = item.artifact_name or _artifact_name_from_url(item.download_url)
    family = item.family or policy.family
    download_auth = item.download_auth or policy.download_auth
    storage_relative_path = item.storage_relative_path or "/".join((family, artifact_name))
    worker_version = item.worker_version
    if worker_version is not None:
        worker_version = worker_version.strip() or None
    if worker_version is None and family in {"rss", "embedding"}:
        worker_version = item.latest_version

    return item.model_copy(
        update={
            "artifact_name": artifact_name,
            "family": family,
            "download_auth": download_auth,
            "storage_relative_path": storage_relative_path,
            "worker_version": worker_version,
            "platform": _normalize_platform(item.platform),
            "arch": _normalize_arch(item.arch),
            "runtime_bundle": _normalize_runtime_bundle(item.runtime_bundle),
        }
    )


def _catalog_release_key(
    item: WorkerReleaseCatalogEntrySchema,
) -> tuple[str, str, str, str, str | None]:
    return (
        _require_value(item.family, "family"),
        item.product,
        _normalize_platform(item.platform),
        _normalize_arch(item.arch),
        _normalize_runtime_bundle(item.runtime_bundle),
    )


def _catalog_item_rank(
    item: WorkerReleaseCatalogEntrySchema,
) -> tuple[tuple[int, tuple[int, int, int], str], object, str]:
    return (
        _version_sort_key(_artifact_version(item)),
        item.published_at,
        _require_value(item.artifact_name, "artifact_name"),
    )


def _build_manifest_read(
    item: WorkerReleaseCatalogEntrySchema,
) -> WorkerReleaseManifestRead:
    return WorkerReleaseManifestRead(
        artifact_name=_require_value(item.artifact_name, "artifact_name"),
        family=_require_value(item.family, "family"),
        product=item.product,
        platform=_normalize_platform(item.platform),
        arch=_normalize_arch(item.arch),
        latest_version=item.latest_version,
        minimum_supported_version=item.minimum_supported_version,
        worker_version=item.worker_version,
        artifact_kind=item.artifact_kind,
        sha256=item.sha256,
        runtime_bundle=_normalize_runtime_bundle(item.runtime_bundle),
        download_auth=_require_value(item.download_auth, "download_auth"),
        download_url=item.download_url,
        release_notes_url=item.release_notes_url,
        published_at=item.published_at,
    )


def _resolve_storage_root() -> Path:
    configured_root = os.getenv(WORKER_RELEASE_STORAGE_ROOT_ENV, "").strip()
    if configured_root:
        return Path(configured_root).resolve()

    configured_catalog_path = os.getenv(WORKER_RELEASE_CATALOG_PATH_ENV, "").strip()
    if configured_catalog_path:
        return Path(configured_catalog_path).expanduser().resolve().parent

    legacy_manifest_path = os.getenv(WORKER_RELEASE_MANIFEST_PATH_ENV, "").strip()
    if legacy_manifest_path:
        return Path(legacy_manifest_path).expanduser().resolve().parent

    backend_root = Path(__file__).resolve().parents[3]
    return backend_root.joinpath("var", "worker-releases").resolve()


def _artifact_name_from_url(download_url: str) -> str:
    parsed = urlparse(download_url)
    candidate = Path(parsed.path).name
    if candidate:
        return candidate
    raise WorkerReleaseCatalogError(f"Unable to derive artifact_name from {download_url}")


def _resolved_worker_version(item: WorkerReleaseCatalogEntrySchema) -> str:
    return (item.worker_version or item.latest_version).strip()


def _artifact_version(item: WorkerReleaseCatalogEntrySchema) -> str:
    artifact_name = (item.artifact_name or "").strip()
    match = _ARTIFACT_VERSION_PATTERN.search(artifact_name)
    if match is not None:
        return match.group(1)
    return item.latest_version


def _version_sort_key(value: str) -> tuple[int, tuple[int, int, int], str]:
    normalized = value.strip()
    match = _SEMVER_PATTERN.fullmatch(normalized)
    if match is None:
        return (0, (0, 0, 0), normalized)
    return (
        1,
        tuple(int(match.group(index)) for index in range(1, 4)),
        normalized,
    )


def _normalize_platform(value: str) -> str:
    normalized = value.strip().lower()
    aliases = {
        "linux": "linux",
        "darwin": "macos",
        "macos": "macos",
        "osx": "macos",
        "windows": "windows",
        "win32": "windows",
    }
    return aliases.get(normalized, normalized)


def _normalize_arch(value: str) -> str:
    normalized = value.strip().lower()
    aliases = {
        "x64": "x86_64",
        "x86_64": "x86_64",
        "amd64": "x86_64",
        "arm64": "aarch64",
        "aarch64": "aarch64",
    }
    return aliases.get(normalized, normalized)


def _normalize_runtime_bundle(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if not normalized or normalized == "none":
        return None
    return normalized


def _require_value(value: str | None, field_name: str) -> str:
    if value is None or not value.strip():
        raise WorkerReleaseCatalogError(f"Missing required release catalog field {field_name}")
    return value
