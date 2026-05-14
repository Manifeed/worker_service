from __future__ import annotations

import re

from shared_backend.domain.article_identity import normalize_article_identity_text

AUTHOR_LIST_SPLIT_PATTERN = re.compile(r"\s*[;,]\s*")
AUTHOR_CONJUNCTION_SPLIT_PATTERN = re.compile(r"\s*(?:&|\band\b|\bet\b)\s*", flags=re.IGNORECASE)
LEADING_BYLINE_PATTERN = re.compile(r"^(?:(?:par|by)\s+)+", flags=re.IGNORECASE)
INLINE_EDITORIAL_SEPARATOR_PATTERNS = (
    re.compile(r"\bedited by\b", flags=re.IGNORECASE),
    re.compile(r"\bexecutive producers? are\b", flags=re.IGNORECASE),
    re.compile(r"\bexecutive producer is\b", flags=re.IGNORECASE),
)
LEADING_EDITORIAL_PREFIX_PATTERNS = (
    re.compile(r"^(?:de notre)\s+", flags=re.IGNORECASE),
    re.compile(r"^(?:notre)\s+", flags=re.IGNORECASE),
    re.compile(r"^(?:propos recueillis par)\s+", flags=re.IGNORECASE),
    re.compile(r"^(?:recueilli(?:e|es|s)? par)\s+", flags=re.IGNORECASE),
    re.compile(r"^(?:reported by)\s+", flags=re.IGNORECASE),
    re.compile(r"^(?:text(?:\s+by)?|texte(?:\s+de)?)\s+", flags=re.IGNORECASE),
)
LEADING_WITH_PATTERN = re.compile(r"^(?:avec|with)\s+(.+)$", flags=re.IGNORECASE)
TRAILING_WITH_PATTERN = re.compile(r"^(.+?)\s+(?:avec|with)\s+(.+)$", flags=re.IGNORECASE)
ROLE_LABEL_PATTERN = re.compile(
    r"^(?:correspondance|correspondant(?:e)?|correspondent|special correspondent|envoy(?:e|é)e?\s+sp(?:e|é)cial(?:e)?)\b",
    flags=re.IGNORECASE,
)
TRAILING_ROLE_PATTERN = re.compile(
    r"^(.+?)(?:\s*[,/-]\s*|\s+)(?:correspondance|correspondant(?:e)?|correspondent|special correspondent|envoy(?:e|é)e?\s+sp(?:e|é)cial(?:e)?)\b.*$",
    flags=re.IGNORECASE,
)
TRAILING_LOCATION_PATTERN = re.compile(
    r"^(.+?)(?:\s*[,/-]\s*|\s+)(?:in|at|a|à|au|aux|en|dans|depuis|sur)\s+.+$",
    flags=re.IGNORECASE,
)
ROLE_LOCATION_PATTERN = re.compile(
    r"^(?:correspondance|correspondant(?:e)?|correspondent|special correspondent|envoy(?:e|é)e?\s+sp(?:e|é)cial(?:e)?)\s+(?:in|at|a|à|au|aux|en|dans|depuis|sur)\s+.+$",
    flags=re.IGNORECASE,
)
GENERIC_EDITORIAL_LABEL_PATTERN = re.compile(
    r"^(?:a|an|the|un|une|le|la|les)\s+.+\b(?:reporter|editor|staff|team|desk|bureau|newsroom|redaction|rédaction|editorial)\b.*$",
    flags=re.IGNORECASE,
)
NAME_WORD_PATTERN = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]+(?:[-'’][A-Za-zÀ-ÖØ-öø-ÿ]+)*")
INITIAL_TOKEN_PATTERN = re.compile(r"(?:[A-Za-zÀ-ÖØ-öø-ÿ]\.)+(?:-[A-Za-zÀ-ÖØ-öø-ÿ]\.)*")
PARENTHETICAL_CHUNK_PATTERN = re.compile(r"\s*\([^()]*\)")
DOMAIN_FRAGMENT_PATTERN = re.compile(
    r"(?:^|[\s(])(?:www\.)?[a-z0-9-]+\.(?:com|fr|org|net|io|co|info|tv|fm|be|ch|de|uk|eu)(?:$|[\s)])",
    flags=re.IGNORECASE,
)
LOCATION_PREFIXES = frozenset({"in", "at", "a", "à", "au", "aux", "en", "dans", "depuis", "sur"})
NAME_PARTICLES = frozenset(
    {"de", "du", "des", "del", "della", "di", "da", "van", "von", "bin", "ibn", "al", "la", "le"}
)
DESCRIPTOR_CUTOFF_WORDS = frozenset(
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


def normalize_display_name(value: str) -> str | None:
    normalized_value = " ".join(value.strip().strip("\"'").split())
    return normalized_value or None


def looks_like_standalone_author(value: str) -> bool:
    if is_discardable_author_fragment(value):
        return False
    parts = value.split()
    if len(parts) >= 2:
        return True
    compact_value = re.sub(r"[^A-Za-z0-9]+", "", value)
    return bool(compact_value) and value == value.upper() and len(compact_value) <= 10


def looks_like_named_author(value: str) -> bool:
    if starts_with_role_label(value):
        return False
    return looks_like_standalone_author(value)


def is_discardable_author_fragment(value: str) -> bool:
    normalized_value = normalize_article_author_name(value)
    if normalized_value in {"text"}:
        return True
    if starts_with_role_label(value) and not ROLE_LOCATION_PATTERN.match(value):
        return True
    if GENERIC_EDITORIAL_LABEL_PATTERN.match(value):
        return True
    if contains_domain_fragment(value):
        return True
    if is_location_fragment(value):
        return True
    return False


def starts_with_role_label(value: str) -> bool:
    return ROLE_LABEL_PATTERN.match(value) is not None


def contains_domain_fragment(value: str) -> bool:
    return DOMAIN_FRAGMENT_PATTERN.search(value) is not None


def is_location_fragment(value: str) -> bool:
    parts = value.split()
    if len(parts) < 2:
        return False
    first_token = parts[0]
    if first_token != first_token.casefold():
        return False
    normalized_first_token = normalize_article_author_name(first_token)
    return normalized_first_token in LOCATION_PREFIXES


def strip_leading_editorial_prefixes(value: str) -> str | None:
    candidate = value
    while True:
        updated_candidate = candidate
        for pattern in LEADING_EDITORIAL_PREFIX_PATTERNS:
            updated_candidate = pattern.sub("", updated_candidate).strip()
        if updated_candidate == candidate:
            return normalize_display_name(updated_candidate)
        candidate = updated_candidate


def prepare_author_source_value(value: str) -> str:
    prepared_value = value
    for pattern in INLINE_EDITORIAL_SEPARATOR_PATTERNS:
        prepared_value = pattern.sub(" ; ", prepared_value)
    return prepared_value


def strip_parenthetical_chunks(value: str) -> str | None:
    candidate = value
    while True:
        updated_candidate = PARENTHETICAL_CHUNK_PATTERN.sub("", candidate).strip()
        if updated_candidate == candidate:
            return normalize_display_name(updated_candidate)
        candidate = updated_candidate


def has_descriptor_cutoff_cue(value: str) -> bool:
    normalized_tokens = [
        normalized_token
        for token in value.split()
        if (normalized_token := normalize_article_author_name(token)) is not None
    ]
    if len(normalized_tokens) <= 1:
        return False
    return any(
        normalized_token in DESCRIPTOR_CUTOFF_WORDS or normalized_token in LOCATION_PREFIXES
        for normalized_token in normalized_tokens[1:]
    )


def extract_leading_name_fragment(value: str) -> str | None:
    extracted_tokens: list[str] = []
    saw_name_token = False
    for token in value.split():
        cleaned_token = token.strip(",;:/")
        if not cleaned_token:
            continue
        normalized_token = normalize_article_author_name(cleaned_token)
        if normalized_token is None:
            break
        if normalized_token in DESCRIPTOR_CUTOFF_WORDS or normalized_token in LOCATION_PREFIXES:
            break
        if normalized_token in NAME_PARTICLES and extracted_tokens:
            extracted_tokens.append(cleaned_token)
            continue
        if is_name_like_token(cleaned_token):
            extracted_tokens.append(cleaned_token)
            saw_name_token = True
            continue
        break
    while extracted_tokens:
        trailing_normalized_token = normalize_article_author_name(extracted_tokens[-1])
        if trailing_normalized_token not in NAME_PARTICLES:
            break
        extracted_tokens.pop()
    if not saw_name_token:
        return None
    return normalize_display_name(" ".join(extracted_tokens))


def is_name_like_token(value: str) -> bool:
    return (
        INITIAL_TOKEN_PATTERN.fullmatch(value) is not None
        or NAME_WORD_PATTERN.fullmatch(value) is not None
    )
