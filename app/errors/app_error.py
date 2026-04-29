from __future__ import annotations

from typing import Any


class AppError(Exception):
    status_code = 500
    code = "app_error"
    default_message = "Application error"

    def __init__(
        self,
        message: str | None = None,
        *,
        details: Any | None = None,
        status_code: int | None = None,
        code: str | None = None,
    ) -> None:
        self.message = message or self.default_message
        self.details = details
        if status_code is not None:
            self.status_code = status_code
        if code is not None:
            self.code = code
        super().__init__(self.message)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.details is not None:
            payload["details"] = self.details
        return payload


class BadRequestError(AppError):
    status_code = 400
    code = "bad_request"
    default_message = "Bad request"


class AuthenticationError(AppError):
    status_code = 401
    code = "authentication_error"
    default_message = "Authentication failed"


class AuthorizationError(AppError):
    status_code = 403
    code = "authorization_error"
    default_message = "Access denied"


class NotFoundError(AppError):
    status_code = 404
    code = "not_found"
    default_message = "Resource not found"


class ConflictError(AppError):
    status_code = 409
    code = "conflict"
    default_message = "Conflict"


class UnprocessableEntityError(AppError):
    status_code = 422
    code = "unprocessable_entity"
    default_message = "Request payload is invalid"


class UpstreamServiceError(AppError):
    status_code = 502
    code = "upstream_service_error"
    default_message = "Upstream service error"
