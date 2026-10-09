# 026. Optional GlitchTip via `SENTRY_DSN` presence

**Status:** Accepted  
**Date:** 2026-09-19  
**Scope:** Process-role error reporting (OPS-10); not a replacement for Phase 4 telemetry

## Context

The `api`, `migrate`, `maintain`, and `relay` roles need opt-in error reporting without
a second APM stack and without leaking payloads, claim tokens, bearer tokens, or the DSN. A separate
boolean flag and an optional extra would break "one image, environment only". The stack is not
FastAPI: framework integrations would capture request bodies and
`X-Queue-Claim-Token`.

## Decision

The only client is the official `sentry_sdk` (a required runtime dependency).
The backend is GlitchTip over the Sentry protocol; SaaS Sentry is not required. A non-empty
`SENTRY_DSN` is the only enablement switch; unset, empty, or whitespace leaves the
SDK uninitialized. Initialization runs once per process from
`maybe_init_error_reporting` in each role `run()` after `--help`. The DSN is a secret
(`Secret`; do not log it). `SENTRY_DSN_FILE` is resolved by the ADR 025 entrypoint; the host
uses `SENTRY_DSN`. Do not send payloads, claim tokens, bearer secrets, the DSN,
or SQL and HTTP breadcrumbs. Tracing, Replay, Profiling, and auto-enabling integrations
are off. Initialization failure is fail-open (the role keeps running).

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| `SENTRY_ENABLED` + DSN | Two flags; an empty DSN with the flag true is ambiguous |
| Extra `queue[sentry]` | Enablement would require a rebuild and would break the single image |
| FastAPI/ASGI/SQLAlchemy integrations | Leaks the claim token, body, and SQL |
| Second vendor / OTEL exporter | A forbidden second APM stack |
| Init in `cli.py` | `--help` must not open the network or other resources |

## Consequences

**Positive:** Environment-only opt-in in every role; GlitchTip without SaaS lock-in; leak
controls match SEC-03 and SEC-05.

**Negative / trade-offs:** The SDK is always in the image; without a DSN the cost is the import only.
Fail-open means a broken DSN does not take the role down.

**Follow-up:** Do not enable Performance, Replay, or Profiling until that choice is a separate
ADR. Do not replace logs, metrics, and traces from [observability.md](../../05-operations/03-observability.md).

## References

- [ADR 025](025-container-file-secrets.md)
- [observability — optional error reporting](../../05-operations/03-observability.md#optional-error-reporting)
- [security.md](../../05-operations/01-security.md)
