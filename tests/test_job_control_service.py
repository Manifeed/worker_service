from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from shared_backend.errors.custom_exceptions import JobStateError
from shared_backend.schemas.jobs.job_schema import JobControlCommandRead, JobStatusRead
from app.services import job_control_service


@dataclass
class _FakeJobRecord:
    status: str


class _FakeSession:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_pause_job_requeues_processing_tasks(monkeypatch) -> None:
    db = _FakeSession()
    seen: list[tuple[str, str]] = []

    monkeypatch.setattr(
        job_control_service,
        "get_worker_job_record",
        lambda db, *, job_id: _FakeJobRecord(status="processing"),
    )
    monkeypatch.setattr(
        job_control_service,
        "requeue_processing_tasks_for_job",
        lambda db, *, job_id: seen.append(("requeue", job_id)),
    )
    monkeypatch.setattr(
        job_control_service,
        "set_worker_job_status",
        lambda db, *, job_id, status: seen.append((status, job_id)) or True,
    )
    monkeypatch.setattr(
        job_control_service,
        "get_worker_job_status_read",
        lambda db, *, job_id: _job_status(job_id=job_id, status="paused"),
    )

    result = job_control_service.pause_job(db, job_id="job-1")

    assert result.status == "paused"
    assert seen == [("requeue", "job-1"), ("paused", "job-1")]
    assert db.commits == 1
    assert db.rollbacks == 0


def test_resume_job_refreshes_status(monkeypatch) -> None:
    db = _FakeSession()
    seen: list[tuple[str, str]] = []

    monkeypatch.setattr(
        job_control_service,
        "get_worker_job_record",
        lambda db, *, job_id: _FakeJobRecord(status="paused"),
    )
    monkeypatch.setattr(
        job_control_service,
        "refresh_worker_job_status",
        lambda db, *, job_id: seen.append(("refresh", job_id)),
    )
    monkeypatch.setattr(
        job_control_service,
        "get_worker_job_status_read",
        lambda db, *, job_id: _job_status(job_id=job_id, status="queued"),
    )

    result = job_control_service.resume_job(db, job_id="job-1")

    assert result.status == "queued"
    assert seen == [("refresh", "job-1")]
    assert db.commits == 1
    assert db.rollbacks == 0


def test_cancel_job_clears_automation_reference(monkeypatch) -> None:
    db = _FakeSession()
    seen: list[tuple[str, str]] = []

    monkeypatch.setattr(
        job_control_service,
        "get_worker_job_record",
        lambda db, *, job_id: _FakeJobRecord(status="processing"),
    )
    monkeypatch.setattr(
        job_control_service,
        "cancel_active_tasks_for_job",
        lambda db, *, job_id: seen.append(("cancel_tasks", job_id)),
    )
    monkeypatch.setattr(
        job_control_service,
        "set_worker_job_status",
        lambda db, *, job_id, status: seen.append((status, job_id)) or True,
    )
    monkeypatch.setattr(
        job_control_service,
        "clear_admin_job_automation_references",
        lambda db, *, job_id: seen.append(("clear_refs", job_id)),
    )
    monkeypatch.setattr(
        job_control_service,
        "get_worker_job_status_read",
        lambda db, *, job_id: _job_status(job_id=job_id, status="cancelled"),
    )

    result = job_control_service.cancel_job(db, job_id="job-1")

    assert result.status == "cancelled"
    assert seen == [
        ("cancel_tasks", "job-1"),
        ("cancelled", "job-1"),
        ("clear_refs", "job-1"),
    ]
    assert db.commits == 1


def test_delete_job_returns_delete_command(monkeypatch) -> None:
    db = _FakeSession()
    seen: list[tuple[str, str]] = []

    monkeypatch.setattr(
        job_control_service,
        "get_worker_job_record",
        lambda db, *, job_id: _FakeJobRecord(status="paused"),
    )
    monkeypatch.setattr(
        job_control_service,
        "clear_admin_job_automation_references",
        lambda db, *, job_id: seen.append(("clear_refs", job_id)),
    )
    monkeypatch.setattr(
        job_control_service,
        "delete_worker_job",
        lambda db, *, job_id: seen.append(("delete", job_id)) or True,
    )

    result = job_control_service.delete_job_permanently(db, job_id="job-1")

    assert result == JobControlCommandRead(ok=True, job_id="job-1", status=None, deleted=True)
    assert seen == [("clear_refs", "job-1"), ("delete", "job-1")]
    assert db.commits == 1


def test_pause_job_rejects_completed_job(monkeypatch) -> None:
    db = _FakeSession()
    monkeypatch.setattr(
        job_control_service,
        "get_worker_job_record",
        lambda db, *, job_id: _FakeJobRecord(status="completed"),
    )

    with pytest.raises(JobStateError):
        job_control_service.pause_job(db, job_id="job-1")


def _job_status(*, job_id: str, status: str) -> JobStatusRead:
    now = datetime(2026, 5, 4, 8, 0, tzinfo=timezone.utc)
    return JobStatusRead(
        job_id=job_id,
        job_kind="rss_scrape",
        status=status,
        requested_at=now,
        task_total=3,
        task_processed=1,
        item_total=5,
        item_success=1,
        item_error=0,
        worker_version="worker-v1",
        started_at=now,
        finished_at=(now if status == "cancelled" else None),
        finalized_at=(now if status == "cancelled" else None),
    )
