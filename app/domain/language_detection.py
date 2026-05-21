from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Protocol, Sequence
from urllib.parse import parse_qsl, urlsplit


LOGGER = logging.getLogger(__name__)
DEFAULT_LANGUAGE_MODEL_PATH = "/opt/models/lid.176.ftz"

MONOLINGUAL_COUNTRY_LANGUAGES: dict[str, str] = {
    "fr": "fr",
    "it": "it",
    "de": "de",
    "es": "es",
    "pt": "pt",
    "nl": "nl",
    "pl": "pl",
    "se": "sv",
    "dk": "da",
    "no": "no",
    "fi": "fi",
    "gr": "el",
    "cz": "cs",
    "sk": "sk",
    "hu": "hu",
    "ro": "ro",
    "bg": "bg",
    "tr": "tr",
    "jp": "ja",
    "kr": "ko",
    "cn": "zh",
    "ru": "ru",
    "us": "en",
    "en": "en",
    "uk": "en",
    "gb": "en",
}

URL_LANGUAGE_HINTS: dict[str, str] = {
    "en": "en",
    "fr": "fr",
    "de": "de",
    "nl": "nl",
    "it": "it",
    "es": "es",
    "pt": "pt",
}

MIN_LANGUAGE_CODE_LENGTH = 2
MAX_LANGUAGE_CODE_LENGTH = 3


class FastTextLikeDetector(Protocol):
    def predict(self, text: str, k: int = 1): ...


def detect_article_language(
    *,
    country: str | None,
    title: str,
    summary: str | None,
    urls: Sequence[str] | None = None,
    detector: FastTextLikeDetector | None = None,
) -> str:
    normalized_country = (country or "").strip().casefold()[:2]
    country_language = MONOLINGUAL_COUNTRY_LANGUAGES.get(normalized_country)
    if country_language is not None:
        return country_language
    url_language = infer_language_from_urls(urls)
    if url_language is not None:
        return url_language
    return detect_text_language(text=_build_language_detection_text(title, summary), detector=detector)


def detect_text_language(
    *,
    text: str,
    detector: FastTextLikeDetector | None = None,
) -> str:
    normalized_text = text.strip()
    if not normalized_text:
        return "xx"
    active_detector = detector or get_fasttext_language_detector()
    if active_detector is None:
        return "xx"
    try:
        labels, _scores = active_detector.predict(normalized_text.replace("\n", " "), k=1)
    except Exception:
        return "xx"
    if not labels:
        return "xx"
    label = str(labels[0]).replace("__label__", "").strip().casefold()
    return normalize_fasttext_language_label(label)


@lru_cache(maxsize=1)
def get_fasttext_language_detector() -> FastTextLikeDetector | None:
    model_path = os.getenv("LANGUAGE_FASTTEXT_MODEL_PATH", DEFAULT_LANGUAGE_MODEL_PATH).strip()
    if not model_path:
        _warn_fasttext_unavailable("LANGUAGE_FASTTEXT_MODEL_PATH is not set")
        return None
    try:
        import fasttext  # type: ignore[import-not-found]
    except Exception:
        _warn_fasttext_unavailable("fasttext module is not installed")
        return None
    try:
        return fasttext.load_model(model_path)
    except Exception as exception:
        _warn_fasttext_unavailable(f"unable to load FastText language model: {exception}")
        return None


@lru_cache(maxsize=None)
def _warn_fasttext_unavailable(reason: str) -> None:
    LOGGER.warning("Language detection will fall back to 'xx': %s", reason)


@lru_cache(maxsize=None)
def _warn_unsupported_fasttext_label(label: str) -> None:
    LOGGER.warning(
        "FastText returned unsupported language label '%s' for CHAR(3) storage; storing 'xx' instead",
        label,
    )


def _build_language_detection_text(title: str, summary: str | None) -> str:
    return "\n\n".join(part.strip() for part in (title, summary or "") if part and part.strip())


def infer_language_from_urls(urls: Sequence[str] | None) -> str | None:
    if not urls:
        return None
    for url in urls:
        language = _infer_language_from_single_url(url)
        if language is not None:
            return language
    return None


def _infer_language_from_single_url(url: str | None) -> str | None:
    raw_url = (url or "").strip()
    if not raw_url:
        return None
    parsed_url = urlsplit(raw_url)
    hostname_tokens = [token.strip().casefold() for token in (parsed_url.hostname or "").split(".") if token.strip()]
    for token in hostname_tokens:
        language = URL_LANGUAGE_HINTS.get(token)
        if language is not None:
            return language

    path_tokens = [token.strip().casefold() for token in parsed_url.path.split("/") if token.strip()]
    for token in reversed(path_tokens):
        normalized_token = token.removesuffix(".xml").removesuffix(".rss").removesuffix(".atom")
        language = URL_LANGUAGE_HINTS.get(normalized_token)
        if language is not None:
            return language

    for query_key, query_value in parse_qsl(parsed_url.query, keep_blank_values=True):
        for candidate in (query_key, query_value):
            language = URL_LANGUAGE_HINTS.get(candidate.strip().casefold())
            if language is not None:
                return language
    return None


def normalize_fasttext_language_label(label: str) -> str:
    normalized_label = label.strip().casefold()
    if not (MIN_LANGUAGE_CODE_LENGTH <= len(normalized_label) <= MAX_LANGUAGE_CODE_LENGTH):
        if normalized_label:
            _warn_unsupported_fasttext_label(normalized_label)
        return "xx"
    return normalized_label
