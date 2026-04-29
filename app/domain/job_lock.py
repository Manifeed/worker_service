from __future__ import annotations
from contextlib import contextmanager
import logging
from threading import Lock
from typing import Optional
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session


class JobAlreadyRunning(RuntimeError):
    """Raised when a named job is already in progress."""


logger = logging.getLogger(__name__)
_LOCKS_GUARD = Lock()
_LOCAL_LOCKS: dict[str, Lock] = {}
_PG_LOCK_IDS: dict[str, int] = {
    "rss_patch_feed_enabled": 83001,
    "rss_patch_company_enabled": 83002,
    "rss_sync": 83003,
    "sources_repartition_partitions": 83004,
    "rss_ingest": 83005,
    "sources_enqueue_embeddings": 83006,
    "admin_job_automation_tick": 83007,
}


def _get_local_lock(name: str) -> Lock:
    with _LOCKS_GUARD:
        lock = _LOCAL_LOCKS.get(name)
        if lock is None:
            lock = Lock()
            _LOCAL_LOCKS[name] = lock
        return lock


def _open_pg_lock_connection(db: Session | None) -> Optional[Connection]:
    if db is None:
        return None
    try:
        bind = db.get_bind()
    except Exception:
        return None

    if bind is None:
        return None

    engine: Optional[Engine]
    if isinstance(bind, Connection):
        engine = bind.engine
    else:
        engine = bind

    if not engine or engine.dialect.name != "postgresql":
        return None

    return engine.connect()


def _try_pg_lock(pg_conn: Connection, lock_id: int) -> bool:
    result = pg_conn.execute(
        text("SELECT pg_try_advisory_lock(:lock_id)"),
        {"lock_id": lock_id},
    ).scalar()
    return bool(result)


def _release_pg_lock(pg_conn: Connection, lock_id: int) -> None:
    pg_conn.execute(
        text("SELECT pg_advisory_unlock(:lock_id)"),
        {"lock_id": lock_id},
    )


@contextmanager
def job_lock(db: Session | None, name: str):
    """
    Prevent concurrent execution of the same job.
    Uses an in-process lock and an optional Postgres advisory lock.
    """
    local_lock = _get_local_lock(name)
    acquired_local = local_lock.acquire(blocking=False)
    if not acquired_local:
        raise JobAlreadyRunning(name)

    lock_id = _PG_LOCK_IDS.get(name)
    acquired_pg = False
    pg_lock_conn: Optional[Connection] = None
    try:
        if lock_id is not None:
            pg_lock_conn = _open_pg_lock_connection(db)
            if pg_lock_conn is not None:
                acquired_pg = _try_pg_lock(pg_lock_conn, lock_id)
                if not acquired_pg:
                    raise JobAlreadyRunning(name)
        yield
    finally:
        if lock_id is not None and acquired_pg and pg_lock_conn is not None:
            try:
                _release_pg_lock(pg_lock_conn, lock_id)
            except Exception:
                logger.exception("Failed to release PostgreSQL advisory lock %s", lock_id)
        if pg_lock_conn is not None:
            pg_lock_conn.close()
        local_lock.release()
