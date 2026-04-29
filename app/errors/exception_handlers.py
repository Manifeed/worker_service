from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .app_error import AppError


logger = logging.getLogger(__name__)


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(AppError, app_error_handler)
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, request_validation_error_handler)
    app.add_exception_handler(Exception, unexpected_exception_handler)


def app_error_handler(_: Request, exception: AppError) -> JSONResponse:
    return JSONResponse(
        status_code=exception.status_code,
        content=exception.to_payload(),
    )


def http_exception_handler(_: Request, exception: HTTPException) -> JSONResponse:
    details = exception.detail if isinstance(exception.detail, list) else None
    message = exception.detail if isinstance(exception.detail, str) else exception.status_code
    return JSONResponse(
        status_code=exception.status_code,
        content={
            "code": "http_error",
            "message": str(message),
            **({"details": details} if details is not None else {}),
        },
    )


def request_validation_error_handler(
    _: Request,
    exception: RequestValidationError,
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "code": "validation_error",
            "message": "Validation error",
            "details": _json_safe_validation_errors(exception.errors()),
        },
    )


def unexpected_exception_handler(_: Request, exception: Exception) -> JSONResponse:
    logger.exception("Unhandled application error", exc_info=exception)
    return JSONResponse(
        status_code=500,
        content={
            "code": "internal_error",
            "message": "Internal server error",
        },
    )


def _json_safe_validation_errors(errors: list[dict]) -> list[dict]:
    safe_errors: list[dict] = []
    for error in errors:
        safe_error = dict(error)
        context = safe_error.get("ctx")
        if isinstance(context, dict):
            safe_error["ctx"] = {
                key: str(value)
                for key, value in context.items()
            }
        safe_errors.append(safe_error)
    return safe_errors
