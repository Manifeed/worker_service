from __future__ import annotations

import logging
import types

from app.domain import language_detection as module_under_test


class FakeFastTextDetector:
    def predict(self, text: str, k: int = 1):
        assert "Bundestag" in text
        assert k == 1
        return ["__label__de"], [0.98]


def _reset_language_detector_caches() -> None:
    module_under_test.get_fasttext_language_detector.cache_clear()
    module_under_test._warn_fasttext_unavailable.cache_clear()
    module_under_test._warn_unsupported_fasttext_label.cache_clear()


def test_detect_article_language_uses_monolingual_country_before_fasttext() -> None:
    assert (
        module_under_test.detect_article_language(
            country="fr",
            title="English title should not matter",
            summary=None,
            detector=FakeFastTextDetector(),
        )
        == "fr"
    )


def test_detect_article_language_maps_england_country_code_to_english() -> None:
    assert (
        module_under_test.detect_article_language(
            country="en",
            title="Ignored title",
            summary=None,
            detector=None,
        )
        == "en"
    )


def test_detect_article_language_uses_fasttext_for_ambiguous_country() -> None:
    assert (
        module_under_test.detect_article_language(
            country="eu",
            title="Bundestag stimmt ab",
            summary=None,
            detector=FakeFastTextDetector(),
        )
        == "de"
    )


def test_detect_article_language_uses_url_hint_for_ambiguous_country() -> None:
    assert (
        module_under_test.detect_article_language(
            country="eu",
            title="Short title",
            summary=None,
            urls=["https://www.consilium.europa.eu/en/rss/pressreleases.ashx"],
            detector=None,
        )
        == "en"
    )


def test_detect_article_language_falls_back_to_unknown_without_detector(monkeypatch, caplog) -> None:
    _reset_language_detector_caches()
    monkeypatch.delenv("LANGUAGE_FASTTEXT_MODEL_PATH", raising=False)
    caplog.set_level(logging.WARNING)

    assert module_under_test.detect_article_language(country="eu", title="Text", summary=None, detector=None) == "xx"
    assert module_under_test.detect_article_language(country="be", title="Text", summary=None, detector=None) == "xx"

    warnings = [record.message for record in caplog.records if "LANGUAGE_FASTTEXT_MODEL_PATH" in record.message]
    assert len(warnings) == 1


def test_get_fasttext_language_detector_warns_once_when_model_load_fails(monkeypatch, caplog) -> None:
    _reset_language_detector_caches()
    monkeypatch.setenv("LANGUAGE_FASTTEXT_MODEL_PATH", "/tmp/lid.bin")
    monkeypatch.setitem(
        __import__("sys").modules,
        "fasttext",
        types.SimpleNamespace(load_model=lambda _path: (_ for _ in ()).throw(OSError("boom"))),
    )
    caplog.set_level(logging.WARNING)

    assert module_under_test.get_fasttext_language_detector() is None
    assert module_under_test.get_fasttext_language_detector() is None

    warnings = [record.message for record in caplog.records if "unable to load FastText language model" in record.message]
    assert len(warnings) == 1


def test_normalize_fasttext_language_label_keeps_two_letter_code() -> None:
    assert module_under_test.normalize_fasttext_language_label("en") == "en"


def test_normalize_fasttext_language_label_keeps_three_letter_code() -> None:
    assert module_under_test.normalize_fasttext_language_label("arz") == "arz"


def test_normalize_fasttext_language_label_rejects_four_letter_code(caplog) -> None:
    _reset_language_detector_caches()
    caplog.set_level(logging.WARNING)

    assert module_under_test.normalize_fasttext_language_label("engl") == "xx"
    assert module_under_test.normalize_fasttext_language_label("engl") == "xx"

    warnings = [record.message for record in caplog.records if "unsupported language label 'engl'" in record.message]
    assert len(warnings) == 1
