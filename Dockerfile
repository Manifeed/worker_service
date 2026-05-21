FROM python:3.13-slim AS builder

WORKDIR /build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10

COPY --from=shared_backend_context . /build/shared_backend/
COPY requirements.txt /build/requirements.txt

RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip wheel --no-cache-dir --wheel-dir /tmp/wheels /build/shared_backend \
    && /opt/venv/bin/pip install --no-cache-dir /tmp/wheels/manifeed_shared_backend-*.whl \
    && /opt/venv/bin/pip install --no-cache-dir --timeout 120 --retries 10 -r /build/requirements.txt

RUN mkdir -p /opt/models \
    && python - <<'PY'
from pathlib import Path
from urllib.request import urlretrieve

model_url = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz"
destination = Path("/opt/models/lid.176.ftz")
urlretrieve(model_url, destination)
print(f"downloaded {destination} from {model_url}")
PY

FROM python:3.13-slim

WORKDIR /app

ENV PATH="/opt/venv/bin:$PATH" \
    LANGUAGE_FASTTEXT_MODEL_PATH="/opt/models/lid.176.ftz" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN useradd --create-home --home-dir /home/appuser --shell /usr/sbin/nologin appuser

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /opt/models /opt/models
COPY --chown=appuser:appuser . /app/

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/internal/health').read()"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
