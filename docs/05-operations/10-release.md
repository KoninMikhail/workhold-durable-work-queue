# GitHub release

Releases are cut from `main` with [release-please](https://github.com/googleapis/release-please).
Conventional Commits on `main` open or update a release pull request. Merging that
pull request tags `vX.Y.Z`, updates `CHANGELOG.md`, and publishes the runtime
image plus the coordinated Python client set.

History before `bootstrap-sha` in `release-please-config.json` is ignored.
`.release-please-manifest.json` is `0.0.0` until the first release: that value
means “not released yet”. `initial-version` is `1.0.0`, so the first GitHub
Release is `v1.0.0`. The release pull request then writes `1.0.0` into the
manifest. Later releases bump from there.

## Cycle

1. Land work on `main` with Conventional Commits (`feat`, `fix`, `perf`, and `!` / `BREAKING CHANGE`).
2. `.github/workflows/release.yml` opens a release pull request that bumps
   `pyproject.toml` versions and `CHANGELOG.md`.
3. On that pull request, CI aligns exact-minor client bounds and `__version__`
   (`tools/sync_coordinated_version.py`). Bounds change only when minor or major
   changes. A patch release leaves `>=X.Y.0,<X.(Y+1).0` in place.
4. Merge the release pull request once CI is green.
5. The same workflow creates the GitHub Release, pushes
   `ghcr.io/<owner>/<repo>:X.Y.Z` (runtime image target, no `v` prefix), and publishes
   `release-packages.json` `pypi_packages` in order: core, then producer,
   consumer, and admin.

The coordinated version in the tree is already `1.0.0` (root and client
`pyproject.toml`, `__version__`, and exact-minor core bounds
`>=1.0.0,<1.1.0`). After `v1.0.0` is released, the next bump follows SemVer:
`feat` → minor, `fix` and the other visible types → patch, `BREAKING CHANGE` →
major. The highest bump in the range wins. A `Release-As: X.Y.Z` footer forces
that version. `ci`, `chore`, `test`, and `style` stay out of the changelog and
do not open a release on their own.

## Files

| File | Role |
| --- | --- |
| `release-please-config.json` | Commit types, `initial-version` `1.0.0`, version files |
| `.release-please-manifest.json` | `0.0.0` until `v1.0.0`, then the last released version |
| `release-packages.json` | Publish order and version files the gate checks |
| `CHANGELOG.md` | Release notes written by release-please |
| `.github/workflows/ci.yml` | Tests, qualification gate, image build |
| `.github/workflows/release.yml` | Release pull request, tag, image push, package publish |

The root `project.version` is the source of truth. Client pyprojects and
`__version__` follow it. Role packages depend on core with an exact-minor range
`>=MAJOR.MINOR.0,<MAJOR.(MINOR+1).0`.

## Qualification

Publication runs `tools/client_release_gate.py --require-qualification-pass`
before any upload. A version bump that changes wheel bytes has to refresh
[09-client-release-qualification.md](09-client-release-qualification.md)
(verdict `PASS`, wheel hashes, Git SHA still an ancestor of `HEAD`) or the
publish job fails closed.

## Repository secrets

| Secret | Required | Purpose |
| --- | --- | --- |
| `GH_PACKAGES_TOKEN` | yes, to publish wheels | Classic PAT with `write:packages`. `GITHUB_TOKEN` cannot publish to the Python registry. |
| `GH_PACKAGES_USERNAME` | no | PAT owner when it differs from the repository owner. |
| `RELEASE_PLEASE_TOKEN` | no | PAT with `contents: write` and `pull-requests: write`. Release pull requests and the bound-sync push then trigger CI. Without it, release-please uses `GITHUB_TOKEN` and a minor bump needs a manual commit of `tools/sync_coordinated_version.py`. |

The runtime image is pushed to GHCR with the workflow `GITHUB_TOKEN`.

## Local check

```bash
uv run python tools/sync_coordinated_version.py
uv run python tools/client_release_gate.py --require-qualification-pass
```

`tools/publish_client_packages.py` uploads only when `WORKHOLD_PUBLISH=1`.
