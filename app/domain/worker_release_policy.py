from __future__ import annotations

from dataclasses import dataclass

from shared_backend.errors.custom_exceptions import WorkerReleaseCatalogError


@dataclass(frozen=True)
class WorkerReleaseProductPolicy:
    family: str
    download_auth: str
    required_worker_type: str | None = None


_POLICY_BY_PRODUCT: dict[str, WorkerReleaseProductPolicy] = {
    "crawler_rss_bundle": WorkerReleaseProductPolicy(
        family="rss",
        download_auth="worker_bearer",
        required_worker_type="rss_scrapper",
    ),
}


def resolve_worker_release_product_policy(product: str) -> WorkerReleaseProductPolicy:
    policy = _POLICY_BY_PRODUCT.get(product)
    if policy is None:
        raise WorkerReleaseCatalogError(f"Unknown worker release product: {product}")
    return policy
