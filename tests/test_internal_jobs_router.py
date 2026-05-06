from __future__ import annotations

from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers import internal_jobs_router as internal_jobs_router_module
from shared_backend.schemas.jobs.job_schema import JobControlCommandRead, JobStatusRead


def _build_test_client() -> TestClient:
    app = FastAPI()
    app.include_router(internal_jobs_router_module.internal_jobs_router)
    app.dependency_overrides[internal_jobs_router_module.require_internal_service_token] = lambda: None
    app.dependency_overrides[internal_jobs_router_module.get_workers_read_db_session] = lambda: object()
    app.dependency_overrides[internal_jobs_router_module.get_workers_write_db_session] = lambda: object()
    app.dependency_overrides[internal_jobs_router_module.get_content_read_db_session] = lambda: object()
    return TestClient(app)


def test_job_control_routes_delegate_to_service(monkeypatch) -> None:
    now = datetime(2026, 5, 4, 8, 0, tzinfo=timezone.utc)
    paused_status = JobStatusRead(
        job_id="job-1",
        job_kind="rss_scrape",
        status="paused",
        requested_at=now,
        task_total=3,
        task_processed=1,
        item_total=5,
        item_success=1,
        item_error=0,
        worker_version="worker-v1",
        started_at=now,
        finished_at=None,
        finalized_at=None,
    )
    running_status = paused_status.model_copy(update={"status": "processing"})
    cancelled_status = paused_status.model_copy(
        update={
            "status": "cancelled",
            "finished_at": now,
            "finalized_at": now,
        }
    )
    deleted_command = JobControlCommandRead(
        ok=True,
        job_id="job-1",
        status=None,
        deleted=True,
    )
    seen: dict[str, str] = {}

    monkeypatch.setattr(
        internal_jobs_router_module,
        "pause_job",
        lambda db, *, job_id: _capture(seen, "pause", job_id, paused_status),
    )
    monkeypatch.setattr(
        internal_jobs_router_module,
        "resume_job",
        lambda db, *, job_id: _capture(seen, "resume", job_id, running_status),
    )
    monkeypatch.setattr(
        internal_jobs_router_module,
        "cancel_job",
        lambda db, *, job_id: _capture(seen, "cancel", job_id, cancelled_status),
    )
    monkeypatch.setattr(
        internal_jobs_router_module,
        "delete_job_permanently",
        lambda db, *, job_id: _capture(seen, "delete", job_id, deleted_command),
    )

    client = _build_test_client()

    pause_response = client.post("/internal/jobs/job-1/pause")
    resume_response = client.post("/internal/jobs/job-1/resume")
    cancel_response = client.post("/internal/jobs/job-1/cancel")
    delete_response = client.delete("/internal/jobs/job-1")

    assert pause_response.status_code == 200
    assert pause_response.json()["status"] == "paused"
    assert seen["pause"] == "job-1"

    assert resume_response.status_code == 200
    assert resume_response.json()["status"] == "processing"
    assert seen["resume"] == "job-1"

    assert cancel_response.status_code == 200
    assert cancel_response.json()["status"] == "cancelled"
    assert seen["cancel"] == "job-1"

    assert delete_response.status_code == 200
    assert delete_response.json()["deleted"] is True
    assert seen["delete"] == "job-1"


def _capture(seen: dict[str, str], key: str, value: str, result):
    seen[key] = value
    return result
