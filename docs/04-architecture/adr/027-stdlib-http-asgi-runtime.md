# 027. Stdlib HTTP + pure ASGI API runtime

**Status:** Accepted  
**Date:** 2026-09-19  
**Scope:** API process HTTP stack (`workhold api`); not the client protocol (ADR 016)

## Context

The API role serves HTTP/JSON OpenAPI without widening the Phase 3.1 supply chain.
FastAPI, Starlette, and uvicorn would provide a middleware ecosystem, but they would add packages,
capture request bodies and `X-Queue-Claim-Token` in later integrations
(ADR 026), and blur the boundary that the protocol is OpenAPI, not a framework.

## Decision

The process HTTP front door is the stdlib `ThreadingHTTPServer`
(`QuietThreadingHTTPServer`). The application and admin planes are hand-written pure
ASGI callables, not FastAPI or Starlette. Conformance and SDK tests hit the live
stdlib stack. Serving does not use `uvicorn` or `httpx`. The protocol remains
HTTP/JSON OpenAPI 3.1 (ADR 016).

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| FastAPI / Starlette | A new runtime dependency; the framework would become a hidden specification |
| uvicorn / ASGI server package | Explicitly rejected in Phase 3.2; an extra process and dependency |
| gRPC server | Already rejected by ADR 016 for the application protocol |

## Consequences

**Positive:** A narrow dependency surface; tests match production HTTP;
Sentry does not attach ASGI or FastAPI integrations.

**Negative / trade-offs:** Routing, CORS, graceful shutdown, and OpenAPI sync are
manual. There is no ready-made middleware ecosystem.

**Follow-up:** Do not add an ASGI server package without a new ADR.

## References

- [ADR 016](016-http-openapi-claim-security.md), [ADR 026](026-optional-glitchtip-sentry-dsn.md)
- [09-client-protocol.md](../09-client-protocol.md)
- `src/workhold/roles/api.py`
