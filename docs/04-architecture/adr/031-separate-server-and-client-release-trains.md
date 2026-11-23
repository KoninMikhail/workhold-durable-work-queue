# 031. Separate server and client release trains

**Status:** Accepted  
**Date:** 2026-11-23  
**Scope:** Versioning, tagging, and publication of the server image and the Python client set

## Context

One release-please component versioned the server, the runtime image, and all four
client distributions together. Every release republished the image and the whole
client set to PyPI. Releases 1.0.1–1.0.4 changed only documentation or release
tooling, yet each produced new client wheels with no client code change.

## Decision

- release-please has two components and one combined release pull request.
- Server train: path `.`, component `workhold`, tag `vX.Y.Z`, `CHANGELOG.md`.
  It ignores commits whose files are all under `packages`, `tests`, `tools`,
  `benchmarks`, `docs`, `.github`, `.cursor`, or `.planning`. Its release pushes
  the runtime image.
- Client train: path `packages`, component `workhold-clients`, tag
  `workhold-clients-vX.Y.Z`, `packages/CHANGELOG.md`. Only files under
  `packages/` count. Its release publishes the client set to PyPI.
- The four client distributions keep one coordinated version (ADR 029) and the
  exact-minor core bound. `workhold-client-core` is the client version source.
- `docs`, `ci`, `chore`, `test`, and `style` commits are hidden and never release.

## Alternatives considered

| Option | Why it was not chosen |
| --- | --- |
| Keep one version and skip PyPI when `packages/` is unchanged | Version gaps on PyPI; still releases the server for client-only changes |
| Independent version per client distribution | Breaks the ADR 029 coordinated set; needs a new core bound policy |

## Consequences

**Positive:** A client release means a client change; the image is rebuilt only for server changes.

**Negative / trade-offs:** Server and client version numbers diverge. A commit that
also touches a shared root file (for example `uv.lock`) releases both trains.
Hidden `docs` commits do not appear in any changelog.

## References

- [ADR 029](029-role-split-python-clients.md) (coordinated client set)
- [ADR 030](030-workhold-distribution-names.md) (distribution names)
- [Client SDK guide](../../02-guides/06-client-sdk-ergonomics.md)
