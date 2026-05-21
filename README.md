# Manifeed Worker Service

FastAPI service dedicated to worker orchestration.

It owns:

- worker sessions, task claiming, completion and failure endpoints under `/workers/api`
- RSS result normalization and persistence

Admin job creation, control, and automation now live in `admin_service`.
Rust workers keep calling `/workers/api/...` through the edge proxy.

The service consumes `shared_backend` for shared internal schemas and internal
service token validation helpers.

## Architecture

- `app/main.py`: FastAPI bootstrap and router registration
- `app/database.py`: worker/content/identity DB accessors
- `app/services`: worker gateway, job orchestration, RSS ingestion, automation
- `app/clients/database`: SQL access split by concern
- `app/domain`: pure worker and RSS normalization rules

## Docker

Build from the monorepo root:

```bash
docker build -t manifeed-worker-service -f worker_service/Dockerfile .
```

The runtime image is multi-stage, runs as a non-root user, and installs
`shared_backend` from a wheel built locally from the monorepo. It also embeds
the official fastText `lid.176.ftz` language-identification model at
`/opt/models/lid.176.ftz` and sets `LANGUAGE_FASTTEXT_MODEL_PATH` to that path
by default.

Run locally with:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

The bundled language model can be used during RSS ingestion and for historical
backfill with:

```bash
python3 scripts/backfill_article_languages.py --batch-size 500
```

You can still override `LANGUAGE_FASTTEXT_MODEL_PATH` if you want to point to a
different fastText model file.
