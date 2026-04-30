# Manifeed Worker Service

FastAPI service dedicated to worker orchestration.

It owns:

- worker sessions, task claiming, completion and failure endpoints under `/workers/api`
- admin job creation and job automation behind internal endpoints under `/internal/jobs`
- RSS result normalization/persistence and embedding finalization
- worker release manifests and downloadable artifacts

`public_api` is the browser-facing facade for admin job routes. Rust workers keep calling
`/workers/api/...` through the edge proxy.

The service consumes `shared_backend` for shared internal schemas and internal
service token validation helpers.

## Docker

Build from the monorepo root:

```bash
docker build -t manifeed-worker-service -f worker_service/Dockerfile .
```

The runtime image is multi-stage, runs as a non-root user, and installs
`shared_backend` from a wheel built locally from the monorepo.
