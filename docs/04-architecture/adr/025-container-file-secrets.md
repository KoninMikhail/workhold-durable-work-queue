# 025. Container secrets via `*_FILE` and entrypoint

**Status:** Accepted  
**Date:** 2026-09-19  
**Scope:** Runtime image injection of deployment secrets (DEP-05)

## Context

Production Compose and Kubernetes mount secrets as files, not as inline environment variables.
Reading those files inside Python `from_environ()` would spread injection across host `uv run`
and the container. An arbitrary scan of `*_FILE`, or an external vault client, would widen
the secret surface without need. Image roles are already fixed by ADR 015.

## Decision

Only the runtime `docker/entrypoint.sh` materializes **allowlisted** secrets from
a sibling `NAME_FILE` into `NAME` before `exec workhold`. Host and `uv run workhold` use
`NAME` only. `NAME` and `NAME_FILE` are exclusive: both set, missing, unreadable,
empty, or a non-absolute path fails closed. Allowlist: `DATABASE_URL`,
`QUEUE_API_BEARER_TOKEN`, `QUEUE_API_BEARER_TOKEN_PREVIOUS`, `SENTRY_DSN`.
The previous token and `SENTRY_DSN` are optional when neither `NAME` nor `NAME_FILE` is set.
File contents and resolved values do not appear on stdout or stderr (names only).
This injects deployment secrets that are already documented. It is not a new secret store
and it does not change SEC-01..05 principals.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| `from_environ()` reads `*_FILE` | Host and tests inherit a container-only contract |
| Scan any `*_FILE` | Unpredictable surface and foreign mounts |
| Vault / CSI / sealed-secrets client | A new secret store outside DEP-05 |
| Inline environment variables only in the image | Breaks Compose and Kubernetes file-secret mounts |

## Consequences

**Positive:** One PID 1 resolver; the host CLI has no file-path semantics; fail-closed
exclusivity matches Docker Official Images `file_env`.

**Negative / trade-offs:** The allowlist must be extended explicitly (as with `SENTRY_DSN` in
Phase 10). `NAME_FILE` works only inside the image, not on the host.

**Follow-up:** Add new deployment secrets to the `file_env` allowlist and
`.env.example`, not to the Python loader.

## References

- [ADR 015](015-single-image-multi-role.md)
- [security.md — File secrets](../../05-operations/01-security.md#file-secrets-_file)
- [deployment.md](../../05-operations/02-deployment.md)
- `docker/entrypoint.sh`
