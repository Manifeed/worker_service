from __future__ import annotations

from dataclasses import dataclass
from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from app.clients.database.auth_database_client import get_user_api_key_context_by_hash
from shared_backend.errors.custom_exceptions import (
    ApiAccessDisabledError,
    InactiveUserError,
    InvalidWorkerApiKeyError,
    MissingWorkerBearerTokenError,
)
from app.domain.worker_identity import build_worker_name
from app.utils.auth_utils import hash_secret_token
from database import get_identity_db_session

_worker_bearer_scheme = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class AuthenticatedWorkerContext:
    api_key_id: int
    user_id: int
    owner_email: str
    worker_type: str
    worker_name: str
    api_key_label: str
    api_key_secret_hash: str


def require_authenticated_worker_context(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_worker_bearer_scheme),
    db: Session = Depends(get_identity_db_session),
) -> AuthenticatedWorkerContext:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise MissingWorkerBearerTokenError()

    key_context = get_user_api_key_context_by_hash(
        db,
        key_hash=hash_secret_token(credentials.credentials),
    )
    if key_context is None or key_context.api_key.revoked_at is not None:
        raise InvalidWorkerApiKeyError()
    if not key_context.user.is_active:
        raise InactiveUserError("Worker owner account is inactive", code="worker_owner_inactive")
    if not key_context.user.api_access_enabled:
        raise ApiAccessDisabledError("Worker API access is disabled", code="worker_api_access_disabled")

    return AuthenticatedWorkerContext(
        api_key_id=key_context.api_key.id,
        user_id=key_context.user.id,
        owner_email=key_context.user.email,
        worker_type=key_context.api_key.worker_type,
        worker_name=build_worker_name(
            pseudo=key_context.user.pseudo,
            worker_type=key_context.api_key.worker_type,
            worker_number=key_context.api_key.worker_number,
        )[:100],
        api_key_label=key_context.api_key.label,
        api_key_secret_hash=key_context.key_hash,
    )
