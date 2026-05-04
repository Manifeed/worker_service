from __future__ import annotations

from app.schemas.workers.worker_result_schema import WorkerResultSchema


def test_worker_result_schema_accepts_rss_worker_statuses() -> None:
    success_result = WorkerResultSchema.model_validate(
        {
            "feed_id": 1,
            "job_id": "rss-2026-05-04-08-14-04",
            "status": "success",
            "sources": [],
        }
    )
    not_modified_result = WorkerResultSchema.model_validate(
        {
            "feed_id": 2,
            "job_id": "rss-2026-05-04-08-14-04",
            "status": "not_modified",
            "sources": [],
        }
    )

    assert success_result.status == "success"
    assert not_modified_result.status == "not_modified"
