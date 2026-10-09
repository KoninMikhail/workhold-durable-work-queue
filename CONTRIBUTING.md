# Contributing to Workhold

Workhold is a personal project under the [MIT License](LICENSE). Pull
requests are welcome. They merge into `main` on
[GitHub](https://github.com/KoninMikhail/workhold-durable-work-queue).

## What belongs here

Read the [product boundary](docs/01-concepts/07-product-boundary.md) and
[guarantees](docs/01-concepts/09-guarantees.md) before proposing a behavior
change. Workhold owns its PostgreSQL and does not promise exactly-once
delivery or a distributed transaction with an application's database.

The live contracts are the code, `openapi/`, the
[storage contract](docs/03-reference/02-storage-contract.md), and
[architecture](docs/04-architecture/README.md). These pages are old proposals
and are not the implementation contract:

- [docs/03-reference/03-formats.md](docs/03-reference/03-formats.md)
- [docs/03-reference/04-storage.md](docs/03-reference/04-storage.md)
- [docs/03-reference/05-http-api.md](docs/03-reference/05-http-api.md)

AI-assisted work starts at [AGENTS.md](AGENTS.md).

## Set up

Python 3.13 and [uv](https://docs.astral.sh/uv/). Full steps:
[local setup](docs/00-onboarding/02-local-setup.md).

```bash
uv sync --all-packages --group dev
uv run pytest
```

Docker stack:

```bash
docker compose -f docker-compose.dev.yml up --build
```

Command list: [commands](docs/03-reference/01-commands.md).

## Changes

Branch from `main`. Keep one pull request to one change.

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/).
release-please opens the release pull request from those messages and writes
[CHANGELOG.md](CHANGELOG.md). Do not bump versions or edit `CHANGELOG.md` by
hand.

| Type | When |
| --- | --- |
| `feat` | New behavior |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `refactor` | No behavior change |
| `test` | Tests only |
| `perf` | Performance |
| `ci` | CI and release automation |
| `chore` | Tooling and other chores |

Subject in English, imperative mood, no trailing period. A breaking change
uses `type!:` in the header or a `BREAKING CHANGE:` footer.

Do not commit `.env`, tokens, DSNs, claim tokens, or other secrets.

## Checks

Run the checks that match the change before opening the pull request.

| Change | Command |
| --- | --- |
| Behavior | `uv run pytest` |
| Client packages under `packages/` | `uv run python tools/check_client_operation_ownership.py` and `uv run python tools/client_release_gate.py` |
| Lockfile | `uv lock --check` |

Pull requests to `main` run, in GitHub Actions:

- `uv sync --frozen --all-packages --group dev`
- `uv run pytest tests/unit`
- the client ownership check, the client release-gate tests, and `tools/client_release_gate.py`
- a build of the runtime image

That workflow does not run the full `tests/` tree. Run the suites your change
can break (`integration`, `conformance`, `chaos`, `sdk`) locally.

## Documentation

A behavior change updates the numbered page that describes it. A new page is
linked from [docs/README.md](docs/README.md). An architecture choice with
rejected alternatives is an ADR in `docs/04-architecture/adr/`.

## Pull request

Use the pull request template: a short summary of what and why, and a test
plan a reviewer can follow. The maintainer reviews pull requests to `main`.

## Security and conduct

Report a vulnerability through [SECURITY.md](SECURITY.md). Do not open a
public issue for it.

Expected behavior in issues and pull requests:
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Where to ask for help:
[SUPPORT.md](SUPPORT.md).
