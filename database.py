import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "20"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "40"))
DB_POOL_TIMEOUT_SECONDS = int(os.getenv("DB_POOL_TIMEOUT_SECONDS", "30"))
DB_POOL_RECYCLE_SECONDS = int(os.getenv("DB_POOL_RECYCLE_SECONDS", "1800"))


def _resolve_database_url(primary_env: str, *fallback_env_names: str) -> str:
    database_url = None
    for env_name in (primary_env,) + fallback_env_names:
        candidate = os.getenv(env_name)
        if candidate and candidate.strip():
            database_url = candidate.strip()
            break
    if not database_url:
        raise RuntimeError(f"{primary_env} must be configured")
    if database_url.startswith("postgresql://") and "+psycopg" not in database_url:
        return database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return database_url


def _create_engine(database_url: str):
    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=DB_POOL_SIZE,
        max_overflow=DB_MAX_OVERFLOW,
        pool_timeout=DB_POOL_TIMEOUT_SECONDS,
        pool_recycle=DB_POOL_RECYCLE_SECONDS,
    )


CONTENT_DATABASE_URL = _resolve_database_url("CONTENT_DATABASE_URL", "DATABASE_URL")
IDENTITY_DATABASE_URL = _resolve_database_url("IDENTITY_DATABASE_URL")
WORKERS_DATABASE_URL = _resolve_database_url("WORKERS_DATABASE_URL")

content_engine = _create_engine(CONTENT_DATABASE_URL)
identity_engine = _create_engine(IDENTITY_DATABASE_URL)
workers_engine = _create_engine(WORKERS_DATABASE_URL)

ContentSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=content_engine)
IdentitySessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=identity_engine)
WorkersSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=workers_engine)

# Backward-compatible aliases for older tests and model imports. New application code
# should use the explicit content/identity/workers helpers below.
DATABASE_URL = CONTENT_DATABASE_URL
engine = content_engine
SessionLocal = ContentSessionLocal


class Base(DeclarativeBase):
    pass


def open_content_db_session() -> Session:
    return ContentSessionLocal()


def open_identity_db_session() -> Session:
    return IdentitySessionLocal()


def open_workers_db_session() -> Session:
    return WorkersSessionLocal()


def open_db_session() -> Session:
    return open_content_db_session()


def get_content_db_session() -> Generator[Session, None, None]:
    db = open_content_db_session()
    try:
        yield db
    finally:
        db.close()


def get_identity_db_session() -> Generator[Session, None, None]:
    db = open_identity_db_session()
    try:
        yield db
    finally:
        db.close()


def get_workers_db_session() -> Generator[Session, None, None]:
    db = open_workers_db_session()
    try:
        yield db
    finally:
        db.close()


def get_db_session() -> Generator[Session, None, None]:
    yield from get_content_db_session()
