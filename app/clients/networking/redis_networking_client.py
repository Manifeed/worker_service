from __future__ import annotations

from dataclasses import dataclass
import os
import socket
import ssl
from urllib.parse import unquote, urlparse


DEFAULT_REDIS_URL = "redis://redis:6379/0"
DEFAULT_REDIS_TIMEOUT_SECONDS = 0.2


class RedisCommandError(RuntimeError):
    """Raised when Redis cannot execute a command."""


@dataclass(frozen=True)
class RedisConnectionConfig:
    url: str
    timeout_seconds: float


class RedisNetworkingClient:
    def __init__(self, config: RedisConnectionConfig | None = None) -> None:
        self.config = config or RedisConnectionConfig(
            url=_resolve_redis_url(),
            timeout_seconds=_resolve_redis_timeout_seconds(),
        )

    def ping(self) -> str:
        return self.execute("PING")

    def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
        if ttl_seconds <= 0:
            raise RedisCommandError("Redis TTL must be positive")
        count = int(self.execute("INCR", key))
        if count == 1:
            self.execute("EXPIRE", key, str(ttl_seconds))
        return count

    def execute(self, *parts: str) -> str:
        if not parts:
            raise RedisCommandError("Redis command is empty")

        parsed_url = urlparse(self.config.url)
        host = parsed_url.hostname or "localhost"
        port = parsed_url.port or 6379
        db_index = parsed_url.path.lstrip("/") or "0"
        username = unquote(parsed_url.username) if parsed_url.username else None
        password = unquote(parsed_url.password) if parsed_url.password else None

        try:
            with socket.create_connection(
                (host, port),
                timeout=self.config.timeout_seconds,
            ) as raw_socket:
                active_socket: socket.socket | ssl.SSLSocket = raw_socket
                if parsed_url.scheme == "rediss":
                    ssl_context = ssl.create_default_context()
                    active_socket = ssl_context.wrap_socket(raw_socket, server_hostname=host)

                with active_socket, active_socket.makefile("rwb") as buffer:
                    if password is not None:
                        if username is not None:
                            _send_redis_command(buffer, "AUTH", username, password)
                        else:
                            _send_redis_command(buffer, "AUTH", password)
                        _read_redis_response(buffer)
                    if db_index and db_index != "0":
                        _send_redis_command(buffer, "SELECT", db_index)
                        _read_redis_response(buffer)
                    _send_redis_command(buffer, *parts)
                    return _read_redis_response(buffer)
        except Exception as exception:
            if isinstance(exception, RedisCommandError):
                raise
            raise RedisCommandError(str(exception)) from exception


def _resolve_redis_url() -> str:
    return os.getenv("REDIS_URL", DEFAULT_REDIS_URL).strip() or DEFAULT_REDIS_URL


def _resolve_redis_timeout_seconds() -> float:
    raw_value = os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", str(DEFAULT_REDIS_TIMEOUT_SECONDS))
    try:
        timeout_seconds = float(raw_value)
    except ValueError:
        return DEFAULT_REDIS_TIMEOUT_SECONDS
    if timeout_seconds <= 0:
        return DEFAULT_REDIS_TIMEOUT_SECONDS
    return timeout_seconds


def _send_redis_command(buffer, *parts: str) -> None:
    payload = [f"*{len(parts)}\r\n".encode("utf-8")]
    for part in parts:
        encoded_part = part.encode("utf-8")
        payload.append(f"${len(encoded_part)}\r\n".encode("utf-8"))
        payload.append(encoded_part + b"\r\n")
    buffer.write(b"".join(payload))
    buffer.flush()


def _read_redis_response(buffer) -> str:
    prefix = buffer.read(1)
    if not prefix:
        raise RedisCommandError("Redis closed the connection")
    if prefix in {b"+", b":"}:
        return _read_redis_line(buffer)
    if prefix == b"$":
        size = int(_read_redis_line(buffer))
        if size < 0:
            return ""
        payload = buffer.read(size)
        buffer.read(2)
        return payload.decode("utf-8")
    if prefix == b"-":
        raise RedisCommandError(_read_redis_line(buffer))
    raise RedisCommandError(f"Unsupported Redis response prefix: {prefix!r}")


def _read_redis_line(buffer) -> str:
    return buffer.readline().decode("utf-8").rstrip("\r\n")
