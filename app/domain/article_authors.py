from __future__ import annotations

from collections.abc import Sequence

from app.domain.article_author_rules import (
    AUTHOR_CONJUNCTION_SPLIT_PATTERN,
    AUTHOR_LIST_SPLIT_PATTERN,
    LEADING_BYLINE_PATTERN,
    LEADING_WITH_PATTERN,
    TRAILING_LOCATION_PATTERN,
    TRAILING_ROLE_PATTERN,
    TRAILING_WITH_PATTERN,
    extract_leading_name_fragment,
    has_descriptor_cutoff_cue,
    is_discardable_author_fragment,
    looks_like_named_author,
    looks_like_standalone_author,
    normalize_article_author_name,
    normalize_display_name,
    prepare_author_source_value,
    starts_with_role_label,
    strip_leading_editorial_prefixes,
    strip_parenthetical_chunks,
)


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


def _split_author_candidates(value: str) -> list[str]:
    prepared_value = prepare_author_source_value(value)
    candidates: list[str] = []
    for part in AUTHOR_LIST_SPLIT_PATTERN.split(prepared_value):
        display_part = normalize_display_name(part)
        if display_part is None:
            continue
        conjunction_parts = _split_conjunction_candidates(display_part)
        for conjunction_part in conjunction_parts:
            cleaned_candidate = _clean_author_candidate(conjunction_part)
            if cleaned_candidate is None:
                continue
            if starts_with_role_label(cleaned_candidate) and any(
                not starts_with_role_label(existing_candidate)
                for existing_candidate in candidates
            ):
                continue
            candidates.append(cleaned_candidate)
    return candidates


def _split_conjunction_candidates(value: str) -> list[str]:
    parts = [
        part
        for part in (
            normalize_display_name(part)
            for part in AUTHOR_CONJUNCTION_SPLIT_PATTERN.split(value)
        )
        if part is not None
    ]
    if len(parts) <= 1:
        return [value]
    cleaned_parts = [_clean_author_candidate(part) for part in parts]
    if not all(
        cleaned_part is not None and looks_like_standalone_author(cleaned_part)
        for cleaned_part in cleaned_parts
    ):
        return [value]
    return parts


def _clean_author_candidate(value: str) -> str | None:
    candidate = normalize_display_name(value)
    if candidate is None:
        return None
    candidate = strip_leading_editorial_prefixes(candidate)
    if candidate is None:
        return None
    candidate = LEADING_BYLINE_PATTERN.sub("", candidate).strip()
    if not candidate:
        return None
    candidate = strip_parenthetical_chunks(candidate)
    if candidate is None:
        return None
    leading_with_match = LEADING_WITH_PATTERN.match(candidate)
    if leading_with_match is not None:
        candidate = leading_with_match.group(1).strip()
        if not candidate:
            return None
    trailing_with_match = TRAILING_WITH_PATTERN.match(candidate)
    if trailing_with_match is not None:
        left_candidate = normalize_display_name(trailing_with_match.group(1))
        right_candidate = normalize_display_name(trailing_with_match.group(2))
        for preferred_candidate in (left_candidate, right_candidate):
            if preferred_candidate is None:
                continue
            if not is_discardable_author_fragment(preferred_candidate):
                candidate = preferred_candidate
                break
    trailing_role_match = TRAILING_ROLE_PATTERN.match(candidate)
    if trailing_role_match is not None:
        left_candidate = normalize_display_name(trailing_role_match.group(1))
        if left_candidate is not None and looks_like_named_author(left_candidate):
            candidate = left_candidate
    if not starts_with_role_label(candidate):
        trailing_location_match = TRAILING_LOCATION_PATTERN.match(candidate)
        if trailing_location_match is not None:
            left_candidate = normalize_display_name(trailing_location_match.group(1))
            if left_candidate is not None and looks_like_named_author(left_candidate):
                candidate = left_candidate
    if not starts_with_role_label(candidate) and has_descriptor_cutoff_cue(candidate):
        candidate = extract_leading_name_fragment(candidate) or candidate
    candidate = normalize_display_name(candidate)
    if candidate is None or is_discardable_author_fragment(candidate):
        return None
    return candidate
