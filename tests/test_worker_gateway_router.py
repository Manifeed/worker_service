from __future__ import annotations

from datetime import datetime, timezone
import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers import worker_gateway_router as worker_gateway_router_module
from app.schemas.workers.worker_gateway_schema import WorkerLeaseRead, WorkerSessionOpenRead
from app.services.worker_auth_service import AuthenticatedWorkerContext


def _build_test_client() -> TestClient:
    app = FastAPI()
    app.include_router(worker_gateway_router_module.worker_gateway_router)
    app.dependency_overrides[worker_gateway_router_module.require_authenticated_worker_context] = (
        lambda: AuthenticatedWorkerContext(
            api_key_id=1,
            user_id=1,
            owner_email="worker@example.com",
            worker_type="rss",
            worker_name="worker-rss-1",
            api_key_label="test",
            api_key_secret_hash="secret-hash",
        )
    )
    app.dependency_overrides[worker_gateway_router_module.get_identity_read_db_session] = lambda: object()
    app.dependency_overrides[worker_gateway_router_module.get_workers_write_db_session] = lambda: object()
    app.dependency_overrides[worker_gateway_router_module.get_content_write_db_session] = lambda: object()
    return TestClient(app)


def test_open_session_accepts_flat_worker_payload(monkeypatch) -> None:
    def fake_open_worker_session(identity_db, workers_db, *, worker, payload):
        del identity_db, workers_db, worker
        return WorkerSessionOpenRead(
            session_id="ws_123",
            task_type=payload.task_type,
            worker_version=payload.worker_version,
            expires_at=datetime(2026, 5, 4, 8, 0, tzinfo=timezone.utc),
        )

    monkeypatch.setattr(worker_gateway_router_module, "open_worker_session", fake_open_worker_session)
    client = _build_test_client()

    response = client.post(
        "/workers/api/sessions/open",
        json={
            "task_type": "rss.fetch",
            "worker_version": "0.1.4",
            "session_ttl_seconds": 300,
        },
    )

    assert response.status_code == 200
    assert response.json()["session_id"] == "ws_123"


def test_claim_tasks_accepts_flat_worker_payload(monkeypatch) -> None:
    def fake_claim_worker_session_tasks(workers_db, *, worker, payload):
        del workers_db, worker
        return [
            WorkerLeaseRead(
                lease_id="lease_123",
                trace_id="trace_123",
                task_type=payload.task_type,
                worker_version=payload.worker_version,
                task_id=10,
                execution_id=20,
                payload_ref="rss.fetch:10:20",
                payload={"feeds": []},
                expires_at=datetime(2026, 5, 4, 8, 5, tzinfo=timezone.utc),
                signed_at=datetime(2026, 5, 4, 8, 0, tzinfo=timezone.utc),
                nonce="nonce_123",
                signature="signature_123",
            )
        ]

    monkeypatch.setattr(
        worker_gateway_router_module,
        "claim_worker_session_tasks",
        fake_claim_worker_session_tasks,
    )
    client = _build_test_client()

    response = client.post(
        "/workers/api/tasks/claim",
        json={
            "session_id": "ws_123",
            "task_type": "rss.fetch",
            "worker_version": "0.1.4",
            "count": 1,
            "lease_seconds": 300,
        },
    )

    assert response.status_code == 200
    assert response.json()[0]["lease_id"] == "lease_123"


def test_complete_task_accepts_flat_worker_payload(monkeypatch) -> None:
    captured: dict[str, bytes] = {}

    def fake_complete_worker_session_task(
        content_db,
        workers_db,
        *,
        worker,
        payload,
        raw_request_body,
    ):
        del content_db, workers_db, worker, payload
        captured["raw_request_body"] = raw_request_body

    monkeypatch.setattr(
        worker_gateway_router_module,
        "complete_worker_session_task",
        fake_complete_worker_session_task,
    )
    client = _build_test_client()

    response = client.post(
        "/workers/api/tasks/complete",
        json={
            "session_id": "ws_123",
            "lease_id": "lease_123",
            "trace_id": "trace_123",
            "task_type": "rss.fetch",
            "worker_version": "0.1.4",
            "signed_at": "2026-05-04T08:00:00Z",
            "nonce": "nonce_123",
            "signature": "signature_123",
            "result_payload": {"contract_version": "rss-worker-result", "result_events": []},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert json.loads(captured["raw_request_body"])["session_id"] == "ws_123"


def test_fail_task_accepts_flat_worker_payload(monkeypatch) -> None:
    called = {"value": False}

    def fake_fail_worker_session_task(workers_db, *, worker, payload):
        del workers_db, worker
        called["value"] = payload.error_message == "boom"

    monkeypatch.setattr(
        worker_gateway_router_module,
        "fail_worker_session_task",
        fake_fail_worker_session_task,
    )
    client = _build_test_client()

    response = client.post(
        "/workers/api/tasks/fail",
        json={
            "session_id": "ws_123",
            "lease_id": "lease_123",
            "trace_id": "trace_123",
            "task_type": "rss.fetch",
            "worker_version": "0.1.4",
            "signed_at": "2026-05-04T08:00:00Z",
            "nonce": "nonce_123",
            "signature": "signature_123",
            "error_message": "boom",
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert called["value"] is True
