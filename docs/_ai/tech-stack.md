# Technology stack

Product: **Workhold - Durable work queue**.

| Layer | Choice |
| --- | --- |
| Language | Python ≥3.13 (pin: `.python-version` = 3.13) |
| Package manager | `uv` |
| Package | distribution `workhold`, import `workhold` |
| HTTP / API | stdlib `ThreadingHTTPServer` + ASGI composition ([ADR 027](../04-architecture/adr/027-stdlib-http-asgi-runtime.md)); OpenAPI 3.1 in `openapi/` |
| ORM / migrations | SQLAlchemy 2.x, Alembic, driver `psycopg` (v3) |
| Database | PostgreSQL 18.6 (`postgres:18.6-alpine` in `docker-compose.dev.yml`; [ADR 024](../04-architecture/adr/024-postgresql-18-6-runtime-engine.md)) |
| Tests | `pytest` (dev group) |
| Containers | `Dockerfile` (targets `runtime`, `dev`); runtime via `docker/entrypoint.sh` (`*_FILE` → `NAME`, [ADR 025](../04-architecture/adr/025-container-file-secrets.md)); `docker-compose.dev.yml` with roles `migrate`, `api`, profiles `maintain`/`relay`/`apply` ([02-deployment.md](../05-operations/02-deployment.md)) |
| Error reporting (opt-in) | Required `sentry-sdk`; non-empty `SENTRY_DSN` presence-only enablement; GlitchTip via Sentry protocol — [ADR 026](../04-architecture/adr/026-optional-glitchtip-sentry-dsn.md), [observability.md](../05-operations/03-observability.md#optional-error-reporting) |

## Commands

```bash
uv sync --group dev
uv sync --all-packages --group dev
uv run pytest
uv run workhold --help
uv run workhold api
uv run alembic upgrade head
docker compose -f docker-compose.dev.yml up --build
uv run python tools/check_client_operation_ownership.py
uv run python tools/client_release_gate.py
uv lock --check
```

Command reference: [../03-reference/01-commands.md](../03-reference/01-commands.md). Env: `DATABASE_URL` and allowlisted `*_FILE` siblings (see `.env.example`); on the host, `uv run` uses only `NAME`.

Release: GitHub Actions release-please on `main`. The first release is `1.0.0`
(`initial-version`; the manifest stays `0.0.0` until that tag exists).
Conventional Commits open a release pull request; merging it tags `vX.Y.Z`,
pushes the runtime image to GHCR as `X.Y.Z`, and publishes the client set in
`release-packages.json` order to PyPI with trusted publishing (`id-token: write`).
No API token is stored. Each distribution needs a pending publisher on PyPI for
workflow `release.yml` before the first upload.

API principals: production uses the mounted
`QUEUE_API_PRINCIPALS_MANIFEST_FILE` secret (roles, rotating generations and
exact queue scopes). Legacy `QUEUE_API_BEARER_TOKEN[_PREVIOUS]` is an
ADMIN-only fallback when the manifest is absent.

Claim long polling env (enabled capability): `QUEUE_CLAIM_MAX_WAIT_SECONDS=20`,
`QUEUE_CLAIM_WAIT_FALLBACK_SECONDS=1.0`, `QUEUE_CLAIM_CANCELLATION_PROBE_SECONDS=0.25`,
`QUEUE_CLAIM_MAX_OUTSTANDING_WAITS=64`. Production proxy upstream timeout ≥ 30 s.

### Client packages

| Distribution | Import | Notes |
| --- | --- | --- |
| `workhold-client-core` | `_workhold_client_core`, `workhold_client_testing` | No operation clients; test kit included |
| `workhold-producer` | `workhold_producer` | Extras: `async`, `bridge-postgres` |
| `workhold-consumer` | `workhold_consumer` | Extra: `async` |
| `workhold-admin` | `workhold_admin` | Extra: `async` |

Coordinated version across core + three roles. CI publishes core first; gate
rejects `queue-client` and partial role sets. Guide:
[06-client-sdk-ergonomics.md](../02-guides/06-client-sdk-ergonomics.md).
