---
name: alembic-migration
description: >-
  Add or change a Workhold PostgreSQL schema migration: Alembic revision in
  alembic/versions, SQLAlchemy models, storage contract doc, health.py schema
  compatibility window, migration tests. Use when new table, new column, index,
  constraint, trigger, alembic revision, schema change, migration, storage contract.
---

# Alembic Migration

The physical schema contract is the code plus
`docs/03-reference/02-storage-contract.md`. Read it and
`docs/04-architecture/04-storage-topology.md` before designing a change.
A new table, partitioning choice, or type decision with alternatives is an ADR
(skill `create-adr`; existing storage ADRs: 006, 007, 022, 023, 024).

## Workflow

```
- [ ] 1. Find the current head: uv run alembic heads (or the last entry of BINARY_REVISION_ORDER)
- [ ] 2. Update SQLAlchemy models in src/workhold/storage/ (metadata: src/workhold/db.py)
- [ ] 3. Write the revision file (conventions below)
- [ ] 4. Extend the health.py compatibility window
- [ ] 5. Update docs/03-reference/02-storage-contract.md
- [ ] 6. Tests: contract, migrations, integration upgrade/downgrade
- [ ] 7. Run checks (skill run-checks)
```

## Revision conventions

Follow the newest file, `alembic/versions/2001_claim_long_poll_wakeup.py`:

- File name = revision id = `<number>_<snake_slug>.py`. Numbers are **not** a
  sorted sequence (`0001`, `039`–`044`, `0501`, `1201`, `2001`); the chain is
  defined only by `down_revision`. Ask the user which number to use if the
  planning phase does not fix it.
- Module docstring: one-line summary, `Revision ID`, `Revises`, `Create Date`,
  then what the revision installs and how downgrade reverses it.
- Header block: `revision`, `down_revision`, `branch_labels = None`,
  `depends_on = None` with the typed annotations used in existing files.
- `down_revision` = current head. One linear head; never create a branch.
- Every revision has a working `downgrade()` that reverses `upgrade()` in
  reverse order (for example triggers before functions).
- Constants shared with runtime code carry a comment naming the source
  (`# Must match workhold.infrastructure...`).
- Autogenerate (`uv run alembic revision --autogenerate -m "…"`) is a draft only:
  partitions, triggers, and functions are written by hand with `op.execute`.

## Schema compatibility window (required)

`src/workhold/health.py` declares which revisions this binary accepts:

- append the new id to `BINARY_REVISION_ORDER` (chain order, not file name order);
- set `BINARY_COMPATIBLE_MAX` to the new id.

Without this, `/readyz` reports `schema_above_range` after `migrate` and the `api`
role does not admit traffic.

## Tests

| What | Where |
| --- | --- |
| Model ↔ contract doc metadata | `tests/contracts/test_storage_contract.py` |
| Offline revision shape | `tests/migrations/` (pattern: `test_initial_migration_shape.py`) |
| Real upgrade/downgrade on PostgreSQL 18.6 | `tests/integration/` via `run_alembic(...)` from `tests/integration/conftest.py` (disposable schema) |
| Readiness window | `tests/integration/test_health.py` |

PostgreSQL suites need `TEST_DATABASE_URL` — see skill `run-checks`.

## Do not

- Edit an existing revision that has shipped; add a new one.
- Use `docs/03-reference/04-storage.md` as a contract — it is a superseded proposal.
- Leave `downgrade()` as `pass`.

## Related skills

| Skill | When |
| --- | --- |
| `run-checks` | Which suites to run and how to get PostgreSQL |
| `create-adr` | Storage decision with rejected alternatives |
| `write-tests` | New migration tests |
| `commit-messages` | Commit message; a migration is server code, so it releases the server train |
