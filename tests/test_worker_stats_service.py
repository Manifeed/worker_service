from __future__ import annotations

from app.services import worker_stats_service


def test_read_worker_stats_includes_runtime_counters(monkeypatch) -> None:
    monkeypatch.setattr(worker_stats_service, "count_active_worker_sessions", lambda db: 4)
    monkeypatch.setattr(
        worker_stats_service,
        "count_pending_worker_tasks",
        lambda db, *, task_type: 7 if task_type == "rss.fetch" else 3,
    )
    monkeypatch.setattr(worker_stats_service, "count_expired_worker_claims", lambda db: 2)
    monkeypatch.setattr(
        worker_stats_service,
        "read_worker_runtime_counter_values",
        lambda db: {
            "stale_redis_task_ids_dropped": 5,
            "embedding_tasks_requeued": 6,
            "payload_rebuild_failures": 1,
        },
    )

    stats = worker_stats_service.read_worker_stats(object())

    assert stats.connected_workers == 4
    assert stats.pending_rss_tasks == 7
    assert stats.pending_embedding_tasks == 3
    assert stats.expired_claims == 2
    assert stats.stale_redis_task_ids_dropped == 5
    assert stats.embedding_tasks_requeued == 6
    assert stats.payload_rebuild_failures == 1
