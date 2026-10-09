# Local setup

[Documentation](../README.md) › Onboarding › **Local setup**

Durable Work Queue with named queues, an HTTP API, Alembic migrations, and role-separated Docker Compose. Product semantics are in [../01-concepts/01-overview.md](../01-concepts/01-overview.md).

## Prerequisites

| Need | Why |
| --- | --- |
| Python 3.13 (pin in `.python-version`) | Host CLI and tests |
| [uv](https://docs.astral.sh/uv/) | Package manager |
| Docker and Docker Compose | Container startup and local Postgres |
| Your own PostgreSQL and `DATABASE_URL` | Only if migrations run outside Compose |

## Installation

```bash
git clone <repository-url>
cd queue
uv sync --group dev
```

The command installs the package, Alembic/SQLAlchemy/psycopg, and the dev dependency `pytest`.

Alembic does not read `.env` by itself. On the host, either set `DATABASE_URL` or leave the default from `alembic.ini` (the same DSN as in `.env.example`).

## First CLI run

```bash
uv run queue --help
```

Shows the roles `api`, `migrate`, `maintain`, `relay`, `apply`. Entrypoint: `[project.scripts] queue = queue_service.cli:run`.

Run one role on the host (`DATABASE_URL` and bearer tokens are required):

```bash
uv run queue api
```

Equivalent via the module:

```bash
uv run python -m queue_service --help
uv run python -m queue_service api
```

## Tests

```bash
uv run pytest
```

Test directory: `tests/` (`tool.pytest.ini_options.testpaths`) — unit, integration, conformance, and chaos suites.

### Client SDK (role packages)

Workspace members: `packages/queue-service-client-core`,
`queue-service-producer`, `queue-service-consumer`, `queue-service-admin`.
The `queue-client` prototype has been removed.

```bash
# Sync workspace (core + three role clients editable)
uv sync --all-packages --group dev

# Async HTTP extra on a role package (pulls core[async] → httpx)
uv sync --all-packages --group dev --package queue-service-producer --extra async
```

Install examples for application code (outside this monorepo):

```bash
pip install queue-service-producer
pip install "queue-service-producer[async]"
pip install queue-service-consumer
pip install queue-service-admin
```

Pass explicit bearer tokens; TLS/timeouts via `ClientConfig`. Full surface:
[06-client-sdk-ergonomics.md](../02-guides/06-client-sdk-ergonomics.md).

Client release gate (wheels + no-upload dry-run, no tag/publish):

```bash
uv run python tools/client_release_gate.py
uv run python tools/check_client_operation_ownership.py
```

## Migrations

Alembic reads `DATABASE_URL` or `sqlalchemy.url` from `alembic.ini` (dialect `postgresql+psycopg`). Revisions are in `alembic/versions/` (physical contract, admin ops, Delivery Outbox schema).

| Command | Live database |
| --- | --- |
| `uv run alembic history` | not required |
| `uv run alembic upgrade head` | required |
| `uv run alembic current` | required |

```bash
uv run alembic history
uv run alembic upgrade head
```

## Docker

The dev stack starts `postgres` (`postgres:18.6-alpine`), one-shot `migrate`, and `api` (application plane on port 8080). The admin listener stays on the internal Compose network (not published).

```mermaid
flowchart LR
  pg["postgres 18.6"] --> migrate["migrate"]
  migrate --> api["api :8080"]
```

The official data-directory mount is `/var/lib/postgresql` (data ends up in `18/docker` inside the volume); the named volume is `queue-pgdata-18`.

```bash
docker compose -f docker-compose.dev.yml up --build
```

Stop without removing volumes:

```bash
docker compose -f docker-compose.dev.yml down
```

> **Unsupported.** The leftover named volume `queue-pgdata` and a host PostgreSQL 16 datadir are not supported. After the cutover to 18.6, destroy the old volumes and recreate the environment.

```bash
docker compose -f docker-compose.dev.yml down -v
```

Then run `up --build` again. Do not use a debian/trixie Postgres image and do not run a major version above the pinned 18.6 in this repository.

`Dockerfile` target `runtime` sets `ENTRYPOINT ["/app/entrypoint.sh"]` and `CMD ["--help"]` (`docker/entrypoint.sh`). Local `docker-compose.dev.yml` passes secrets inline via `NAME` (for example `DATABASE_URL`, `QUEUE_API_BEARER_TOKEN`). The `*_FILE` / `NAME_FILE` pattern is for production: the image entrypoint reads the file and materializes `NAME` before the role.

Profiles for one-shot roles:

```bash
docker compose -f docker-compose.dev.yml --profile maintain run --no-deps --rm maintain
docker compose -f docker-compose.dev.yml --profile relay run --no-deps --rm relay
docker compose -f docker-compose.dev.yml --profile apply run --rm apply
```

Profile `apply` is optional and is **not** required for `api` (api does not `depends_on`
apply; the catalog is not read on startup / `/readyz`).

## Where next

- why the service exists — [../01-concepts/01-overview.md](../01-concepts/01-overview.md)
- outbox and inbox — [../01-concepts/10-transactional-outbox.md](../01-concepts/10-transactional-outbox.md), [../01-concepts/11-inbox.md](../01-concepts/11-inbox.md)
- commands and package facts — [../03-reference/01-commands.md](../03-reference/01-commands.md)
- contents — [../README.md](../README.md)
- rules for agents — [../../AGENTS.md](../../AGENTS.md)

---

← [Reading path](01-reading-path.md) · [Contents](../README.md)
