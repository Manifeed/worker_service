from __future__ import annotations

import hashlib
import secrets

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError


SESSION_TOKEN_BYTES = 32
API_KEY_TOKEN_BYTES = 36
SESSION_TOKEN_PREFIX = "msess"  # nosec
API_KEY_PREFIX = "mk"
API_KEY_VISIBLE_PREFIX_LENGTH = 12

_password_hasher = PasswordHasher()


def hash_password(password: str) -> str:
    return _password_hasher.hash(password)


def verify_password(password_hash: str, password: str) -> bool:
    try:
        return _password_hasher.verify(password_hash, password)
    except VerifyMismatchError:
        return False


def generate_session_token() -> str:
    return f"{SESSION_TOKEN_PREFIX}_{secrets.token_urlsafe(SESSION_TOKEN_BYTES)}"


def generate_api_key() -> str:
    return f"{API_KEY_PREFIX}_{secrets.token_urlsafe(API_KEY_TOKEN_BYTES)}"


def hash_secret_token(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_key_prefix(api_key: str) -> str:
    return api_key[:API_KEY_VISIBLE_PREFIX_LENGTH]
