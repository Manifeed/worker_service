from app.services.rss_worker_ingestion_candidates import normalize_article_image_url


def test_normalize_article_image_url_keeps_valid_short_https_url() -> None:
    value = "https://example.com/image.png"

    assert normalize_article_image_url(value) == value


def test_normalize_article_image_url_returns_none_for_too_long_url() -> None:
    value = "https://example.com/" + ("a" * 2000)

    assert normalize_article_image_url(value) is None
