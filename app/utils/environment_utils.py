from __future__ import annotations

import os


def is_development_environment() -> bool:
    for env_var in ("APP_ENV", "ENVIRONMENT", "NODE_ENV"):
        raw_value = os.getenv(env_var)
        if raw_value is None:
            continue
        normalized = raw_value.strip().lower()
        if normalized in {"dev", "development", "local", "test", "testing"}:
            return True
        if normalized in {"prod", "production", "staging"}:
            return False
    return False


def is_production_like_environment() -> bool:
    for env_var in ("APP_ENV", "ENVIRONMENT", "NODE_ENV"):
        raw_value = os.getenv(env_var)
        if raw_value is None:
            continue
        normalized = raw_value.strip().lower()
        if normalized in {"prod", "production", "staging"}:
            return True
        if normalized in {"dev", "development", "local", "test", "testing"}:
            return False
    return False
