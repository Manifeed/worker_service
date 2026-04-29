from __future__ import annotations

from .app_error import (
    AppError,
    AuthenticationError,
    AuthorizationError,
    BadRequestError,
    ConflictError,
    NotFoundError,
    UnprocessableEntityError,
    UpstreamServiceError,
)


class UserNotFoundError(NotFoundError):
    code = "user_not_found"
    default_message = "Unknown user"


class ApiKeyNotFoundError(NotFoundError):
    code = "api_key_not_found"
    default_message = "Unknown API key"


class InvalidCredentialsError(AuthenticationError):
    code = "invalid_credentials"
    default_message = "Invalid credentials"


class MissingSessionTokenError(AuthenticationError):
    code = "missing_session_token"
    default_message = "Missing session token"


class InvalidSessionTokenError(AuthenticationError):
    code = "invalid_session_token"
    default_message = "Invalid session token"


class ExpiredSessionTokenError(AuthenticationError):
    code = "expired_session_token"
    default_message = "Session token expired"


class MissingWorkerBearerTokenError(AuthenticationError):
    code = "missing_worker_bearer_token"
    default_message = "Missing worker bearer token"


class InvalidWorkerApiKeyError(AuthenticationError):
    code = "invalid_worker_api_key"
    default_message = "Invalid worker API key"


class InactiveUserError(AuthorizationError):
    code = "inactive_user"
    default_message = "User account is inactive"


class ApiAccessDisabledError(AuthorizationError):
    code = "api_access_disabled"
    default_message = "API key access is disabled for this account"


class AdminAccessRequiredError(AuthorizationError):
    code = "admin_access_required"
    default_message = "Admin access required"


class CsrfOriginDeniedError(AuthorizationError):
    code = "csrf_origin_denied"
    default_message = "CSRF origin check failed"


class InternalServiceAuthError(AuthorizationError):
    code = "internal_service_auth_failed"
    default_message = "Internal service authentication failed"


class DuplicateUserRegistrationError(ConflictError):
    code = "duplicate_user_registration"
    default_message = "Email or pseudo already registered"


class InvalidPseudoError(UnprocessableEntityError):
    code = "invalid_pseudo"
    default_message = "Invalid pseudo"


class WeakPasswordError(UnprocessableEntityError):
    code = "weak_password"
    default_message = "Password does not match the security policy"


class ApiKeyAllocationError(ConflictError):
    code = "api_key_allocation_failed"
    default_message = "Unable to allocate a stable worker number for this API key"


class JobNotFoundError(NotFoundError):
    code = "job_not_found"
    default_message = "Job not found"


class JobAlreadyRunningError(ConflictError):
    code = "job_already_running"
    default_message = "Job is already running"


class JobEnqueueError(UpstreamServiceError):
    code = "job_enqueue_error"
    default_message = "Unable to enqueue job"


class SourceNotFoundError(NotFoundError):
    code = "source_not_found"
    default_message = "RSS source not found"


class RssRepositorySyncError(UpstreamServiceError):
    code = "rss_repository_sync_error"
    default_message = "RSS repository sync failed"


class RssCatalogParseError(UnprocessableEntityError):
    code = "rss_catalog_parse_error"
    default_message = "RSS catalog payload is invalid"


class RssIconNotFoundError(NotFoundError):
    code = "rss_icon_not_found"
    default_message = "RSS icon not found"


class RssFeedNotFoundError(NotFoundError):
    code = "rss_feed_not_found"
    default_message = "RSS feed not found"


class RssCompanyNotFoundError(NotFoundError):
    code = "rss_company_not_found"
    default_message = "RSS company not found"


class RssFeedToggleForbiddenError(ConflictError):
    code = "rss_feed_toggle_forbidden"
    default_message = "RSS feed toggle is not allowed"


class WorkerProtocolError(BadRequestError):
    code = "worker_protocol_error"
    default_message = "Worker request does not match the expected protocol"


class WorkerSessionNotFoundError(NotFoundError):
    code = "worker_session_not_found"
    default_message = "Worker session does not exist"


class WorkerLeaseNotFoundError(NotFoundError):
    code = "worker_lease_not_found"
    default_message = "Worker lease does not exist"


class WorkerReleaseNotFoundError(NotFoundError):
    code = "worker_release_not_found"
    default_message = "Worker release manifest entry not found"


class WorkerReleaseCatalogError(AppError):
    code = "worker_release_catalog_error"
    default_message = "Worker release catalog is invalid"


class WorkerReleaseDownloadForbiddenError(AuthorizationError):
    code = "worker_release_download_forbidden"
    default_message = "Worker API key does not grant access to this artifact"


class WorkerSignatureError(AuthorizationError):
    code = "worker_signature_error"
    default_message = "Worker request signature is invalid"


class WorkerLeaseStateError(ConflictError):
    code = "worker_lease_state_error"
    default_message = "Worker lease is not in a valid state"


class WorkerTaskNotFoundError(NotFoundError):
    code = "worker_task_not_found"
    default_message = "Worker task does not exist"


class WorkerTaskStateError(ConflictError):
    code = "worker_task_state_error"
    default_message = "Worker task is not in a valid state"


class WorkerTaskValidationError(UnprocessableEntityError):
    code = "worker_task_validation_error"
    default_message = "Worker task payload is invalid"


class RateLimitExceededError(AppError):
    status_code = 429
    code = "rate_limit_exceeded"
    default_message = "Too many requests"
