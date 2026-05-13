import pytest

from app.domain.worker_release_policy import resolve_worker_release_product_policy
from shared_backend.errors.custom_exceptions import WorkerReleaseCatalogError


def test_crawler_rss_bundle_requires_rss_worker_key() -> None:
    policy = resolve_worker_release_product_policy("crawler_rss_bundle")

    assert policy.family == "rss"
    assert policy.download_auth == "worker_bearer"
    assert policy.required_worker_type == "rss_scrapper"


def test_old_embedding_release_product_is_not_supported() -> None:
    with pytest.raises(WorkerReleaseCatalogError):
        resolve_worker_release_product_policy("embedding_" "worker_bundle")
