from __future__ import annotations

import re
from collections.abc import Sequence

from .article_identity import normalize_article_identity_text

_AUTHOR_LIST_SPLIT_PATTERN = re.compile(r"\s*[;,]\s*")
_AUTHOR_CONJUNCTION_SPLIT_PATTERN = re.compile(r"\s*(?:&|\band\b|\bet\b)\s*", flags=re.IGNORECASE)
_LEADING_BYLINE_PATTERN = re.compile(r"^(?:(?:par|by)\s+)+", flags=re.IGNORECASE)
_INLINE_EDITORIAL_SEPARATOR_PATTERNS = (
    re.compile(r"\bedited by\b", flags=re.IGNORECASE),
    re.compile(r"\bexecutive producers? are\b", flags=re.IGNORECASE),
    re.compile(r"\bexecutive producer is\b", flags=re.IGNORECASE),
)
_LEADING_EDITORIAL_PREFIX_PATTERNS = (
    re.compile(r"^(?:de notre)\s+", flags=re.IGNORECASE),
    re.compile(r"^(?:notre)\s+", flags=re.IGNORECASE),
    re.compile(r"^(?:propos recueillis par)\s+", flags=re.IGNORECASE),
    re.compile(r"^(?:recueilli(?:e|es|s)? par)\s+", flags=re.IGNORECASE),
    re.compile(r"^(?:reported by)\s+", flags=re.IGNORECASE),
    re.compile(r"^(?:text(?:\s+by)?|texte(?:\s+de)?)\s+", flags=re.IGNORECASE),
)
_LEADING_WITH_PATTERN = re.compile(r"^(?:avec|with)\s+(.+)$", flags=re.IGNORECASE)
_TRAILING_WITH_PATTERN = re.compile(r"^(.+?)\s+(?:avec|with)\s+(.+)$", flags=re.IGNORECASE)
_ROLE_LABEL_PATTERN = re.compile(
    r"^(?:correspondance|correspondant(?:e)?|correspondent|special correspondent|envoy(?:e|é)e?\s+sp(?:e|é)cial(?:e)?)\b",
    flags=re.IGNORECASE,
)
_TRAILING_ROLE_PATTERN = re.compile(
    r"^(.+?)(?:\s*[,/-]\s*|\s+)(?:correspondance|correspondant(?:e)?|correspondent|special correspondent|envoy(?:e|é)e?\s+sp(?:e|é)cial(?:e)?)\b.*$",
    flags=re.IGNORECASE,
)
_TRAILING_LOCATION_PATTERN = re.compile(
    r"^(.+?)(?:\s*[,/-]\s*|\s+)(?:in|at|a|à|au|aux|en|dans|depuis|sur)\s+.+$",
    flags=re.IGNORECASE,
)
_ROLE_LOCATION_PATTERN = re.compile(
    r"^(?:correspondance|correspondant(?:e)?|correspondent|special correspondent|envoy(?:e|é)e?\s+sp(?:e|é)cial(?:e)?)\s+(?:in|at|a|à|au|aux|en|dans|depuis|sur)\s+.+$",
    flags=re.IGNORECASE,
)
_GENERIC_EDITORIAL_LABEL_PATTERN = re.compile(
    r"^(?:a|an|the|un|une|le|la|les)\s+.+\b(?:reporter|editor|staff|team|desk|bureau|newsroom|redaction|rédaction|editorial)\b.*$",
    flags=re.IGNORECASE,
)
_NAME_WORD_PATTERN = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]+(?:[-'’][A-Za-zÀ-ÖØ-öø-ÿ]+)*")
_INITIAL_TOKEN_PATTERN = re.compile(r"(?:[A-Za-zÀ-ÖØ-öø-ÿ]\.)+(?:-[A-Za-zÀ-ÖØ-öø-ÿ]\.)*")
_PARENTHETICAL_CHUNK_PATTERN = re.compile(r"\s*\([^()]*\)")
_DOMAIN_FRAGMENT_PATTERN = re.compile(
    r"(?:^|[\s(])(?:www\.)?[a-z0-9-]+\.(?:com|fr|org|net|io|co|info|tv|fm|be|ch|de|uk|eu)(?:$|[\s)])",
    flags=re.IGNORECASE,
)
_LOCATION_PREFIXES = frozenset({"in", "at", "a", "à", "au", "aux", "en", "dans", "depuis", "sur"})
_NAME_PARTICLES = frozenset(
    {"de", "du", "des", "del", "della", "di", "da", "van", "von", "bin", "ibn", "al", "la", "le"}
)
_DESCRIPTOR_CUTOFF_WORDS = frozenset(
    {
        "aumonier",
        "aumoniere",
        "diocese",
        "dioceses",
        "diocese de",
        "diocese d",
        "editor",
        "editors",
        "edited",
        "executive",
        "producer",
        "producers",
        "reporter",
        "regional",
        "regionale",
        "special",
        "speciale",
        "speciales",
        "specialiste",
        "correspondance",
        "correspondant",
        "correspondante",
        "hospital",
        "hopital",
        "bureau",
    }
)


def normalize_article_author_name(value: str | None) -> str | None:
    return normalize_article_identity_text(value)


def split_article_author_value(value: str | None) -> list[str]:
    if value is None:
        return []

    raw_value = value.strip()
    if not raw_value:
        return []

    names: list[str] = []
    seen_normalized_names: set[str] = set()
    for part in _split_author_candidates(raw_value):
        candidate = _clean_author_candidate(part)
        if candidate is None:
            continue
        normalized_name = normalize_article_author_name(candidate)
        if normalized_name is None or normalized_name in seen_normalized_names:
            continue
        seen_normalized_names.add(normalized_name)
        names.append(candidate)
    return names


def coerce_article_author_names(
    *,
    author_names: Sequence[str] | None = None,
    author: str | None = None,
) -> list[str]:
    resolved_names: list[str] = []
    seen_normalized_names: set[str] = set()
    raw_values = list(author_names or [])
    if author is not None:
        raw_values.append(author)

    for raw_value in raw_values:
        for candidate in split_article_author_value(raw_value):
            normalized_name = normalize_article_author_name(candidate)
            if normalized_name is None or normalized_name in seen_normalized_names:
                continue
            seen_normalized_names.add(normalized_name)
            resolved_names.append(candidate)
    return resolved_names


def _normalize_display_name(value: str) -> str | None:
    normalized_value = " ".join(value.strip().strip("\"'").split())
    return normalized_value or None


def _split_author_candidates(value: str) -> list[str]:
    prepared_value = _prepare_author_source_value(value)
    candidates: list[str] = []
    for part in _AUTHOR_LIST_SPLIT_PATTERN.split(prepared_value):
        display_part = _normalize_display_name(part)
        if display_part is None:
            continue
        conjunction_parts = _split_conjunction_candidates(display_part)
        for conjunction_part in conjunction_parts:
            cleaned_candidate = _clean_author_candidate(conjunction_part)
            if cleaned_candidate is None:
                continue
            if _starts_with_role_label(cleaned_candidate) and any(
                not _starts_with_role_label(existing_candidate)
                for existing_candidate in candidates
            ):
                continue
            candidates.append(cleaned_candidate)
    return candidates


def _split_conjunction_candidates(value: str) -> list[str]:
    parts = [
        part
        for part in (
            _normalize_display_name(part)
            for part in _AUTHOR_CONJUNCTION_SPLIT_PATTERN.split(value)
        )
        if part is not None
    ]
    if len(parts) <= 1:
        return [value]

    cleaned_parts = [_clean_author_candidate(part) for part in parts]
    if not all(
        cleaned_part is not None and _looks_like_standalone_author(cleaned_part)
        for cleaned_part in cleaned_parts
    ):
        return [value]
    return parts


def _clean_author_candidate(value: str) -> str | None:
    candidate = _normalize_display_name(value)
    if candidate is None:
        return None

    candidate = _strip_leading_editorial_prefixes(candidate)
    if candidate is None:
        return None

    candidate = _LEADING_BYLINE_PATTERN.sub("", candidate).strip()
    if not candidate:
        return None

    candidate = _strip_parenthetical_chunks(candidate)
    if candidate is None:
        return None

    leading_with_match = _LEADING_WITH_PATTERN.match(candidate)
    if leading_with_match is not None:
        candidate = leading_with_match.group(1).strip()
        if not candidate:
            return None

    trailing_with_match = _TRAILING_WITH_PATTERN.match(candidate)
    if trailing_with_match is not None:
        left_candidate = _normalize_display_name(trailing_with_match.group(1))
        right_candidate = _normalize_display_name(trailing_with_match.group(2))
        for preferred_candidate in (left_candidate, right_candidate):
            if preferred_candidate is None:
                continue
            if not _is_discardable_author_fragment(preferred_candidate):
                candidate = preferred_candidate
                break

    trailing_role_match = _TRAILING_ROLE_PATTERN.match(candidate)
    if trailing_role_match is not None:
        left_candidate = _normalize_display_name(trailing_role_match.group(1))
        if left_candidate is not None and _looks_like_named_author(left_candidate):
            candidate = left_candidate

    if not _starts_with_role_label(candidate):
        trailing_location_match = _TRAILING_LOCATION_PATTERN.match(candidate)
        if trailing_location_match is not None:
            left_candidate = _normalize_display_name(trailing_location_match.group(1))
            if left_candidate is not None and _looks_like_named_author(left_candidate):
                candidate = left_candidate

    if not _starts_with_role_label(candidate) and _has_descriptor_cutoff_cue(candidate):
        candidate = _extract_leading_name_fragment(candidate) or candidate

    candidate = _normalize_display_name(candidate)
    if candidate is None or _is_discardable_author_fragment(candidate):
        return None
    return candidate


def _looks_like_standalone_author(value: str) -> bool:
    if _is_discardable_author_fragment(value):
        return False

    parts = value.split()
    if len(parts) >= 2:
        return True

    compact_value = re.sub(r"[^A-Za-z0-9]+", "", value)
    return bool(compact_value) and value == value.upper() and len(compact_value) <= 10


def _looks_like_named_author(value: str) -> bool:
    if _starts_with_role_label(value):
        return False
    return _looks_like_standalone_author(value)


def _is_discardable_author_fragment(value: str) -> bool:
    normalized_value = normalize_article_author_name(value)
    if normalized_value in {"text"}:
        return True
    if _starts_with_role_label(value) and not _ROLE_LOCATION_PATTERN.match(value):
        return True
    if _GENERIC_EDITORIAL_LABEL_PATTERN.match(value):
        return True
    if _contains_domain_fragment(value):
        return True
    if _is_location_fragment(value):
        return True
    return False


def _starts_with_role_label(value: str) -> bool:
    return _ROLE_LABEL_PATTERN.match(value) is not None


def _contains_domain_fragment(value: str) -> bool:
    return _DOMAIN_FRAGMENT_PATTERN.search(value) is not None


def _is_location_fragment(value: str) -> bool:
    parts = value.split()
    if len(parts) < 2:
        return False

    first_token = parts[0]
    if first_token != first_token.casefold():
        return False

    normalized_first_token = normalize_article_author_name(first_token)
    return normalized_first_token in _LOCATION_PREFIXES


def _strip_leading_editorial_prefixes(value: str) -> str | None:
    candidate = value
    while True:
        updated_candidate = candidate
        for pattern in _LEADING_EDITORIAL_PREFIX_PATTERNS:
            updated_candidate = pattern.sub("", updated_candidate).strip()
        if updated_candidate == candidate:
            return _normalize_display_name(updated_candidate)
        candidate = updated_candidate


def _prepare_author_source_value(value: str) -> str:
    prepared_value = value
    for pattern in _INLINE_EDITORIAL_SEPARATOR_PATTERNS:
        prepared_value = pattern.sub(" ; ", prepared_value)
    return prepared_value


def _strip_parenthetical_chunks(value: str) -> str | None:
    candidate = value
    while True:
        updated_candidate = _PARENTHETICAL_CHUNK_PATTERN.sub("", candidate).strip()
        if updated_candidate == candidate:
            return _normalize_display_name(updated_candidate)
        candidate = updated_candidate


def _has_descriptor_cutoff_cue(value: str) -> bool:
    normalized_tokens = [
        normalized_token
        for token in value.split()
        if (normalized_token := normalize_article_author_name(token)) is not None
    ]
    if len(normalized_tokens) <= 1:
        return False
    return any(
        normalized_token in _DESCRIPTOR_CUTOFF_WORDS or normalized_token in _LOCATION_PREFIXES
        for normalized_token in normalized_tokens[1:]
    )


def _extract_leading_name_fragment(value: str) -> str | None:
    extracted_tokens: list[str] = []
    saw_name_token = False

    for token in value.split():
        cleaned_token = token.strip(",;:/")
        if not cleaned_token:
            continue

        normalized_token = normalize_article_author_name(cleaned_token)
        if normalized_token is None:
            break
        if normalized_token in _DESCRIPTOR_CUTOFF_WORDS or normalized_token in _LOCATION_PREFIXES:
            break
        if normalized_token in _NAME_PARTICLES and extracted_tokens:
            extracted_tokens.append(cleaned_token)
            continue
        if _is_name_like_token(cleaned_token):
            extracted_tokens.append(cleaned_token)
            saw_name_token = True
            continue
        break

    while extracted_tokens:
        trailing_normalized_token = normalize_article_author_name(extracted_tokens[-1])
        if trailing_normalized_token not in _NAME_PARTICLES:
            break
        extracted_tokens.pop()

    if not saw_name_token:
        return None
    return _normalize_display_name(" ".join(extracted_tokens))


def _is_name_like_token(value: str) -> bool:
    return _INITIAL_TOKEN_PATTERN.fullmatch(value) is not None or _NAME_WORD_PATTERN.fullmatch(
        value
    ) is not None
