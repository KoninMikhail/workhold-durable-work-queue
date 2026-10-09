# 016. HTTP/JSON OpenAPI and split claim credentials

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Application/admin protocol and lease authorization

## Context

Applications need a language-neutral contract. A claim secret in the URL would leak through
access logs and traces, and a secret token alone would make safe resource correlation harder.

## Decision

Use HTTP/JSON REST with OpenAPI 3.1 as the machine source of truth.
The application API is versioned under `/v1`; the private admin API is versioned under `/admin/v1`.
Lease operations use a public `claim_id` in the path and a separate random
`claim_token` in a protected header, plus claim-generation validation.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| gRPC only | Higher integration and tooling cost for the initial consumers |
| HTTP and gRPC together | Doubles the conformance surface before it is needed |
| Claim token in URL | Ordinary logs and traces expose the bearer capability |
| Secret token without public claim ID | Weak operational correlation and routing |

## Consequences

**Positive:** Portable generated clients, inspectable contracts, and safer
claim secrets.

**Negative / trade-offs:** Header handling and token redaction are required;
OpenAPI and the implementation must stay in sync.

**Follow-up:** Phase 3 defines the exact paths, header name, schemas, and
conformance.

## References

- [Client protocol](../09-client-protocol.md)
- [Security](../../05-operations/01-security.md)
- [ADR 027](027-stdlib-http-asgi-runtime.md) — process HTTP stack, not the protocol
