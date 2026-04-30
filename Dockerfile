FROM python:3.11-slim AS builder

WORKDIR /build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10

COPY shared_backend/ /build/shared_backend/
COPY worker_service/requirements.txt /build/worker_service/requirements.txt

RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip wheel --no-cache-dir --wheel-dir /tmp/wheels /build/shared_backend \
    && /opt/venv/bin/pip install --no-cache-dir /tmp/wheels/manifeed_shared_backend-*.whl \
    && /opt/venv/bin/pip install --no-cache-dir --timeout 120 --retries 10 -r /build/worker_service/requirements.txt

FROM python:3.11-slim

WORKDIR /app

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN useradd --create-home --home-dir /home/appuser --shell /usr/sbin/nologin appuser

COPY --from=builder /opt/venv /opt/venv
COPY worker_service/ /app/

RUN chown -R appuser:appuser /app /opt/venv

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/internal/health').read()"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
