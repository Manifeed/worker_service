from __future__ import annotations

import os
import secrets

from fastapi import Request

from app.errors.custom_exceptions import InternalServiceAuthError

INTERNAL_SERVICE_TOKEN_HEADER = "x-manifeed-internal-token"
_LOCAL_ENVIRONMENTS = {"", "dev", "development", "local", "test", "testing"}


def require_internal_service_token(request: Request) -> None:
    expected_token = os.getenv("INTERNAL_SERVICE_TOKEN", "").strip()
    if not expected_token:
        if _is_local_environment():
            return
        raise InternalServiceAuthError("INTERNAL_SERVICE_TOKEN is not configured")
    if len(expected_token) < 32 and not _is_local_environment():
        raise InternalServiceAuthError("INTERNAL_SERVICE_TOKEN is too weak")
    received_token = request.headers.get(INTERNAL_SERVICE_TOKEN_HEADER, "").strip()
    if not received_token or not secrets.compare_digest(received_token, expected_token):
        raise InternalServiceAuthError()


def _is_local_environment() -> bool:
    environment = os.getenv("APP_ENV", os.getenv("ENVIRONMENT", "")).strip().lower()
    if environment:
        return environment in _LOCAL_ENVIRONMENTS
    require_token = os.getenv("REQUIRE_INTERNAL_SERVICE_TOKEN", "").strip().lower()
    if require_token in {"1", "true", "yes", "on"}:
        return False
    return True
