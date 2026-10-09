# Commands and package

[Documentation](../README.md) › Reference › **Commands**

Facts from `pyproject.toml`, sources, and Docker files.

## Package

| Field | Value |
| --- | --- |
| Distribution | `queue` |
| Import | `queue_service` |
| Version | `1.0.0` |
| Python | `>=3.13` (pin `.python-version`: `3.13`) |
| Script | `queue` → `queue_service.cli:run` |
| Runtime dependencies | `alembic`, `sqlalchemy`, `psycopg[binary]`, `sentry-sdk` |
| Dev group | `pytest>=8` |

The import package name is not `queue`, so it does not collide with the stdlib.

## Env

The process and `from_environ` read the **plain name** — the value itself (DSN, token), not a path.
Do not substitute `*_FILE` for `NAME`, and do not set it on the host: it is an absolute
path to a file **only** when the secret is mounted via Compose `secrets:` / a Kubernetes
Secret volume. Then `entrypoint.sh` reads the file and materializes the plain name
before `exec` of the role. `NAME` and `NAME_FILE` are exclusive: setting both at once is forbidden.
On the host (`uv run queue`, Alembic, local Compose) set the plain name.

| Variable | Purpose | Secret file (image only) |
| --- | --- | --- |
| `DATABASE_URL` | PostgreSQL DSN (`postgresql+psycopg://…`). For Alembic on the host: if it is unset, `sqlalchemy.url` from `alembic.ini` is used. | `DATABASE_URL_FILE` |
| `QUEUE_API_BEARER_TOKEN` | API bearer token (current). | `QUEUE_API_BEARER_TOKEN_FILE` |
| `QUEUE_API_BEARER_TOKEN_PREVIOUS` | Optional previous bearer during rotation. | `QUEUE_API_BEARER_TOKEN_PREVIOUS_FILE` |
| `SENTRY_DSN` | Optional GlitchTip/Sentry DSN. Non-empty enables `sentry_sdk`; unset/empty — SDK off. Secret; do not log. | `SENTRY_DSN_FILE` |
| `QUEUE_SCHEDULE_HORIZON_SECONDS` | Maximum future offset for producer enqueue and Complete `spawn[]` `available_at`. Integer **0..86400**; default and absolute maximum **86400**. Deployment can only tighten the ceiling. Non-secret; see `.env.example`. | — |
| `QUEUE_CATALOG_PATH` | Absolute path to the JSON named-queue catalog for the `apply` role. Required for `queue apply`. Not a secret; **no** `QUEUE_CATALOG_PATH_FILE`. Not a `DeploymentSettings` field. | — |
| `QUEUE_APPLY_REPLICA_CEILING` | Replica ceiling for the apply pool (default `1`). | — |
| `QUEUE_APPLY_POOL_CEILING` | Connection ceiling for the apply pool (default `2`). | — |
| `QUEUE_APPLY_LOCK_DEADLINE_SECONDS` | Deadline advisory lock apply (default `30`). | — |

Name examples: `.env.example`. The `.env` file is not committed to git.

## Commands

| Command | What it does |
| --- | --- |
| `uv sync --group dev` | Install the package, Alembic, and pytest |
| `uv run queue --help` | Help for process roles (`api`, `migrate`, `maintain`, `relay`, `apply`) |
| `uv run queue <role>` | Start one role (`api`, `migrate`, `maintain`, `relay`, `apply`) |
| `uv run queue apply` | One-shot ensure-exists named queues from `QUEUE_CATALOG_PATH`. The catalog is **not baked into the image**; **not** GitOps reconcile; **not** `api` startup / `/readyz`. Exit: `0` success (including all-already-exist), `2` usage/invalid catalog, `4` lock timeout, `5` dependency, `1` unexpected |
| `uv run python -m queue_service --help` | Same as `uv run queue --help` |
| `uv run pytest` | Tests from `tests/` |
| `uv run alembic history` | List of revisions (no database connection) |
| `uv run alembic current` | Current revision in the database |
| `uv run alembic revision --autogenerate -m "…"` | New revision from `Base.metadata` |
| `uv run alembic upgrade head` | Apply migrations |
| `uv run alembic downgrade -1` | Roll back one revision |
| `docker compose -f docker-compose.dev.yml up --build` | `postgres`, one-shot `migrate`, `api` (port 8080) |
| `docker compose -f docker-compose.dev.yml --profile apply run --rm apply` | One-shot catalog apply (mount example JSON) |
| `docker compose -f docker-compose.dev.yml down` | Stop compose |

Roles `maintain`, `relay`, and `apply` in compose run via profiles (`--profile maintain`,
`--profile relay`, `--profile apply`) or `compose run`. `api` does not `depends_on`
apply.

## Docker

| Artifact | Fact |
| --- | --- |
| `Dockerfile` target `runtime` | `uv sync --frozen --no-dev --no-editable`, `ENTRYPOINT ["/app/entrypoint.sh"]`, `CMD ["--help"]` |
| `Dockerfile` target `dev` | `uv sync --frozen --group dev`, `CMD ["python", "-m", "queue_service"]` |
| `docker-compose.dev.yml` | Services `postgres` (`postgres:18.6-alpine`), `migrate`, `api`; profiles `maintain`, `relay`, `apply` |

Base image: `ghcr.io/astral-sh/uv:python3.13-bookworm-slim`.

Compose sets `DATABASE_URL=postgresql+psycopg://queue:queue@postgres:5432/queue`. Local Postgres in compose: user/password/db `queue`, port `5432`.

## Sources

| Path | Purpose |
| --- | --- |
| `src/queue_service/cli.py` | Multi-role dispatcher (`api`, `migrate`, `maintain`, `relay`, `apply`) |
| `src/queue_service/roles/` | Role entrypoints (`apply.py` — catalog ensure-exists) |
| `src/queue_service/domain/catalog.py` | Parse/validate named-queue catalog JSON |
| `src/queue_service/db.py` | `Base` / `Base.metadata` for Alembic |
| `alembic.ini` | Alembic config, default `sqlalchemy.url` |
| `alembic/env.py` | Migration engine, URL from `DATABASE_URL` |
| `alembic/versions/` | Revisions of the physical contract and admin ops |
| `docker/entrypoint.sh` | `*_FILE` → `NAME`, then `exec queue` |
| `tests/` | unit, integration, conformance, chaos |
