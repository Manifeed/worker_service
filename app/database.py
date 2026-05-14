from typing import Generator

from sqlalchemy.orm import DeclarativeBase, Session

from shared_backend.database import (
    configure_database_access,
    get_db_session as shared_get_db_session,
    open_db_session as shared_open_db_session,
)

_CONTENT_DATABASE = configure_database_access(
    write_env="CONTENT_WRITE_DATABASE_URL",
    read_env="CONTENT_READ_DATABASE_URL",
    write_fallback_env_names=("CONTENT_DATABASE_URL", "DATABASE_URL"),
    read_fallback_env_names=("CONTENT_DATABASE_URL", "DATABASE_URL"),
)
_IDENTITY_DATABASE = configure_database_access(
    write_env="IDENTITY_READ_DATABASE_URL",
    write_fallback_env_names=("IDENTITY_DATABASE_URL", "IDENTITY_WRITE_DATABASE_URL"),
)
_WORKERS_DATABASE = configure_database_access(
    write_env="WORKERS_WRITE_DATABASE_URL",
    read_env="WORKERS_READ_DATABASE_URL",
    write_fallback_env_names=("WORKERS_DATABASE_URL",),
    read_fallback_env_names=("WORKERS_DATABASE_URL",),
)

CONTENT_READ_DATABASE_URL = _CONTENT_DATABASE.read_url
CONTENT_WRITE_DATABASE_URL = _CONTENT_DATABASE.write_url
CONTENT_DATABASE_URL = CONTENT_READ_DATABASE_URL
IDENTITY_READ_DATABASE_URL = _IDENTITY_DATABASE.read_url
IDENTITY_DATABASE_URL = IDENTITY_READ_DATABASE_URL
WORKERS_READ_DATABASE_URL = _WORKERS_DATABASE.read_url
WORKERS_WRITE_DATABASE_URL = _WORKERS_DATABASE.write_url
WORKERS_DATABASE_URL = WORKERS_READ_DATABASE_URL

ContentReadSessionLocal = _CONTENT_DATABASE.read_session_factory
ContentWriteSessionLocal = _CONTENT_DATABASE.write_session_factory
IdentityReadSessionLocal = _IDENTITY_DATABASE.read_session_factory
WorkersReadSessionLocal = _WORKERS_DATABASE.read_session_factory
WorkersWriteSessionLocal = _WORKERS_DATABASE.write_session_factory


class Base(DeclarativeBase):
    pass


def open_content_read_db_session() -> Session:
    return shared_open_db_session(ContentReadSessionLocal)


def open_content_write_db_session() -> Session:
    return shared_open_db_session(ContentWriteSessionLocal)


def open_workers_write_db_session() -> Session:
    return shared_open_db_session(WorkersWriteSessionLocal)


def get_content_read_db_session() -> Generator[Session, None, None]:
    yield from shared_get_db_session(ContentReadSessionLocal)


def get_content_write_db_session() -> Generator[Session, None, None]:
    yield from shared_get_db_session(ContentWriteSessionLocal)


def get_identity_read_db_session() -> Generator[Session, None, None]:
    yield from shared_get_db_session(IdentityReadSessionLocal)


def get_workers_read_db_session() -> Generator[Session, None, None]:
    yield from shared_get_db_session(WorkersReadSessionLocal)


def get_workers_write_db_session() -> Generator[Session, None, None]:
    yield from shared_get_db_session(WorkersWriteSessionLocal)
