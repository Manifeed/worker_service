from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass(frozen=True)
class UserRecord:
    id: int
    email: str
    pseudo: str
    pp_id: int
    password_hash: str
    role: str
    is_active: bool
    api_access_enabled: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class UserApiKeyRecord:
    id: int
    user_id: int
    label: str
    worker_type: str
    worker_number: int
    key_prefix: str
    last_used_at: datetime | None
    revoked_at: datetime | None
    created_at: datetime


@dataclass(frozen=True)
class UserApiKeyContextRecord:
    api_key: UserApiKeyRecord
    user: UserRecord
    key_hash: str


def get_user_api_key_context_by_hash(
    db: Session,
    *,
    key_hash: str,
) -> UserApiKeyContextRecord | None:
    row = (
        db.execute(
            text(
                """
                SELECT
                    api_keys.id AS api_key_id,
                    api_keys.user_id,
                    api_keys.label,
                    api_keys.worker_type,
                    api_keys.worker_number,
                    api_keys.key_prefix,
                    api_keys.key_hash,
                    api_keys.last_used_at,
                    api_keys.revoked_at,
                    api_keys.created_at AS api_key_created_at,
                    users.id,
                    users.email,
                    users.pseudo,
                    users.pp_id,
                    users.password_hash,
                    users.role,
                    users.is_active,
                    users.api_access_enabled,
                    users.created_at,
                    users.updated_at
                FROM user_api_keys AS api_keys
                JOIN users
                    ON users.id = api_keys.user_id
                WHERE api_keys.key_hash = :key_hash
                """
            ),
            {"key_hash": key_hash},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return UserApiKeyContextRecord(
        api_key=_map_user_api_key_context_key(row),
        user=_map_user(row),
        key_hash=str(row["key_hash"]),
    )


def touch_user_api_key_last_used(db: Session, *, api_key_id: int) -> None:
    db.execute(
        text(
            """
            UPDATE user_api_keys
            SET last_used_at = now()
            WHERE id = :api_key_id
                AND revoked_at IS NULL
            """
        ),
        {"api_key_id": api_key_id},
    )


def upsert_api_key_worker_usage(
    db: Session,
    *,
    api_key_id: int,
    worker_name: str,
    worker_type: str,
    worker_version: str | None,
) -> None:
    db.execute(
        text(
            """
            INSERT INTO api_key_worker_usages (
                api_key_id,
                worker_name,
                worker_type,
                worker_version
            ) VALUES (
                :api_key_id,
                :worker_name,
                :worker_type,
                :worker_version
            )
            ON CONFLICT (api_key_id, worker_name)
            DO UPDATE SET
                worker_type = EXCLUDED.worker_type,
                worker_version = EXCLUDED.worker_version,
                last_seen_at = now(),
                use_count = api_key_worker_usages.use_count + 1
            """
        ),
        {
            "api_key_id": api_key_id,
            "worker_name": worker_name,
            "worker_type": worker_type,
            "worker_version": worker_version,
        },
    )


def _map_user(row: Mapping[str, Any]) -> UserRecord:
    return UserRecord(
        id=int(row["id"]),
        email=str(row["email"]),
        pseudo=str(row["pseudo"]),
        pp_id=int(row["pp_id"]),
        password_hash=str(row["password_hash"]),
        role=str(row["role"]),
        is_active=bool(row["is_active"]),
        api_access_enabled=bool(row["api_access_enabled"]),
        created_at=_normalize_datetime(row["created_at"]) or datetime.now(timezone.utc),
        updated_at=_normalize_datetime(row["updated_at"]) or datetime.now(timezone.utc),
    )


def _map_user_api_key_context_key(row: Mapping[str, Any]) -> UserApiKeyRecord:
    return UserApiKeyRecord(
        id=int(row["api_key_id"]),
        user_id=int(row["user_id"]),
        label=str(row["label"]),
        worker_type=str(row["worker_type"]),
        worker_number=int(row["worker_number"]),
        key_prefix=str(row["key_prefix"]),
        last_used_at=_normalize_datetime(row["last_used_at"]),
        revoked_at=_normalize_datetime(row["revoked_at"]),
        created_at=_normalize_datetime(row["api_key_created_at"]) or datetime.now(timezone.utc),
    )


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
