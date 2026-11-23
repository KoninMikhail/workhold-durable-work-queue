---
name: run-checks
description: >-
  Choose and run the Workhold checks that match a change: pytest suites
  (unit, contracts, sdk, migrations, integration, concurrency, conformance, chaos),
  client ownership check, client release gate, uv lock --check, and the PostgreSQL
  they need. Use when verify the change, run tests, which tests to run, before PR,
  before commit, CI failed, TEST_DATABASE_URL.
---

# Run Checks

Pick the checks from the **changed paths**, run them, report exact results.
Sources: `CONTRIBUTING.md` § Checks, `.github/workflows/ci.yml`,
`docs/03-reference/01-commands.md`, `docs/05-operations/06-chaos-testing.md`.

## Workflow

```
- [ ] 1. List changed paths: git status + git diff --stat (vs main for a branch)
- [ ] 2. Map paths to checks (table below)
- [ ] 3. Offline checks first, then PostgreSQL suites
- [ ] 4. Report: command, pass/fail counts, first failure; never hide a failure
```

## Install

```bash
uv sync --all-packages --group dev
```

## Path → checks

| Changed paths | Run |
| --- | --- |
| Any Python under `src/` | `uv run pytest tests/unit -q` + the PostgreSQL suites of the touched area |
| `src/workhold/api/`, `src/workhold/security/`, `openapi/` | `uv run pytest tests/contracts -q`, `uv run python tools/check_client_operation_ownership.py`, `tests/conformance` |
| `alembic/`, `src/workhold/storage/`, `src/workhold/db.py`, `docs/03-reference/02-storage-contract.md` | `uv run pytest tests/migrations tests/contracts/test_storage_contract.py tests/test_alembic.py -q` + `tests/integration` |
| `src/workhold/infrastructure/`, `application/`, `intake/`, `delivery/` | `tests/integration`, `tests/concurrency` for lease/claim/complete/cancel races |
| `packages/` | `uv run pytest tests/sdk -q`, `uv run python tools/check_client_operation_ownership.py`, `uv run python tools/client_release_gate.py`, `tests/conformance` for client behavior |
| `pyproject.toml`, `packages/*/pyproject.toml`, `uv.lock` | `uv lock --check` + full offline set |
| `Dockerfile`, `docker/` | `docker build --target runtime -t workhold:local .` |
| `docs/` only | `uv run pytest tests/docs tests/contracts -q` (doc-linked contract tests) |

When unsure of the blast radius, run the CI subset plus every PostgreSQL suite of
the touched package.

## CI subset (what a pull request runs)

```bash
uv run pytest tests/unit -q
uv run python tools/check_client_operation_ownership.py
uv run pytest tests/sdk/test_package_release_matrix.py tests/sdk/test_client_release_gate.py tests/sdk/test_sync_coordinated_version.py -q
uv run python tools/client_release_gate.py
```

CI does **not** run `integration`, `concurrency`, `conformance`, `chaos`, or the full
`sdk` tree. Run the ones the change can break locally.

## PostgreSQL suites

`tests/integration`, `tests/concurrency`, `tests/conformance`, `tests/chaos` (and
`tests/sdk/producer/bridge/test_postgres_store.py`) require `TEST_DATABASE_URL` and
**fail** (never skip) without it. Plain `uv run pytest` over the whole tree
therefore fails without a database.

PowerShell (this workstation):

```powershell
docker compose -f docker-compose.dev.yml up -d postgres
$env:TEST_DATABASE_URL = 'postgresql+psycopg://queue:queue@127.0.0.1:5432/queue'
uv run pytest tests/integration -q
```

Each test uses a disposable Alembic schema, so the dev database is reused safely.

Isolated compose runners (`docker-compose.conformance.yml`, always torn down):

| Command | Purpose |
| --- | --- |
| `uv run python tools/run_storage_integration.py` | Storage integration inside a one-off service container, asserts PostgreSQL 18.6 |
| `uv run python tools/run_conformance.py --pytest [ARGS]` | Boot service + Postgres, run selected pytest with `TEST_DATABASE_URL` and `CONFORMANCE_BASE_URL`; report in `.conformance-out/` |

## Rules

- Do not invent commands; this table and the sources above are the list.
- Do not mark a check passed without running it. If Docker or PostgreSQL is
  unavailable, say which suites were not run.
- An environmental failure (Docker down, `TEST_DATABASE_URL` unset) is not an
  invariant failure — report it as such.

## Related skills

| Skill | When |
| --- | --- |
| `write-tests` | A check exposes missing coverage |
| `fix-bug` | A check fails on behavior |
| `alembic-migration`, `api-contract-change`, `client-sdk-change` | Area-specific steps |
