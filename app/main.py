from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from shared_backend.errors.exception_handlers import register_exception_handlers
from app.routers.internal_jobs_router import internal_jobs_router
from app.routers.internal_worker_stats_router import internal_worker_stats_router
from app.routers.worker_gateway_router import worker_gateway_router
from app.routers.worker_release_router import worker_release_router
from shared_backend.schemas.internal.service_schema import InternalServiceHealthRead
from app.services.admin_job_automation_service import (
    start_admin_job_automation_scheduler,
    stop_admin_job_automation_scheduler,
)
from shared_backend.utils.logging_utils import (
    configure_service_logging,
    create_request_logging_middleware,
)
from shared_backend.utils.public_url import require_public_base_url


@asynccontextmanager
async def lifespan(app: FastAPI):
    del app
    start_admin_job_automation_scheduler()
    try:
        yield
    finally:
        stop_admin_job_automation_scheduler()


def create_app() -> FastAPI:
    configure_service_logging("worker-service")
    require_public_base_url()
    app = FastAPI(title="Manifeed Worker Service", lifespan=lifespan)
    app.middleware("http")(
        create_request_logging_middleware(
            service_name="worker-service",
            route_class="worker-api",
        )
    )
    app.include_router(worker_gateway_router)
    app.include_router(worker_release_router)
    app.include_router(internal_jobs_router)
    app.include_router(internal_worker_stats_router)

    @app.get("/internal/health", response_model=InternalServiceHealthRead)
    def read_internal_health() -> InternalServiceHealthRead:
        return InternalServiceHealthRead(service="worker-service", status="ok")

    register_exception_handlers(app)
    return app


app = create_app()
