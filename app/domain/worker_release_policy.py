from __future__ import annotations

from dataclasses import dataclass

from shared_backend.errors.custom_exceptions import WorkerReleaseCatalogError


@dataclass(frozen=True)
class WorkerReleaseProductPolicy:
    family: str
    download_auth: str
    required_worker_type: str | None = None


_POLICY_BY_PRODUCT: dict[str, WorkerReleaseProductPolicy] = {
    "workers_desktop_app": WorkerReleaseProductPolicy(
        family="desktop",
        download_auth="public",
    ),
    "manifeed-workers-desktop": WorkerReleaseProductPolicy(
        family="desktop",
        download_auth="public",
    ),
    "rss_worker_bundle": WorkerReleaseProductPolicy(
        family="rss",
        download_auth="worker_bearer",
        required_worker_type="rss_scrapper",
    ),
    "embedding_worker_bundle": WorkerReleaseProductPolicy(
        family="embedding",
        download_auth="worker_bearer",
        required_worker_type="source_embedding",
    ),
}

_DESKTOP_RELEASE_ORDER: dict[tuple[str, str], int] = {
    ("macos", "aarch64"): 0,
    ("linux", "x86_64"): 1,
    ("linux", "aarch64"): 2,
}


def resolve_worker_release_product_policy(product: str) -> WorkerReleaseProductPolicy:
    policy = _POLICY_BY_PRODUCT.get(product)
    if policy is None:
        raise WorkerReleaseCatalogError(f"Unknown worker release product: {product}")
    return policy


def desktop_release_sort_key(platform: str, arch: str) -> tuple[int, str, str]:
    return (_DESKTOP_RELEASE_ORDER.get((platform, arch), 99), platform, arch)


def build_desktop_release_presentation(
    *,
    platform: str,
    arch: str,
    artifact_name: str,
) -> tuple[str, str, str, str | None]:
    if platform == "macos":
        return (
            "Manifeed Workers.app",
            "macOS 14+ · Apple Silicon",
            "Download .dmg",
            None,
        )

    arch_label = {
        "x86_64": "amd64",
        "aarch64": "arm64",
    }.get(arch, arch)
    return (
        "Desktop app",
        f"Debian / Ubuntu · {arch_label}",
        "Download .deb",
        f"sudo apt install ./{artifact_name}",
    )
