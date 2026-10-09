# 013. Separate producer, worker, and admin security planes

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Authentication, authorization and credential blast radius

## Context

Producer, worker, and admin operations have very different impact. A shared
API key would let a compromised worker change retry policy or replay dead
letters, and a free-form worker ID does not establish identity.

## Decision

Deployment-issued service principals have explicit operation and named-queue
scopes. Producer, worker, relay, observer, and admin credentials are distinct.
Admin uses the private control plane. The diagnostic worker ID is derived from
the authenticated identity; the claim token remains an ephemeral lease
capability.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| One application-wide secret | Excessive blast radius and no actor attribution |
| Worker ID as authorization | A spoofable diagnostic value, and not a rotating fence |
| Full queue-service-owned IAM/SSO | Overengineering for a per-application service |
| Network trust without authentication | Any reachable pod could claim or complete work |

## Consequences

**Positive:** Least privilege, a reliable audit identity, and isolated admin
impact.

**Negative / trade-offs:** More credentials, rotation procedures, and
queue-scope configuration.

**Follow-up:** Phase 3 chooses deployable bearer-token and/or mTLS adapters
without changing authorization semantics.

## References

- [Security contract](../../05-operations/01-security.md)
- [Fenced leases ADR](005-fenced-leases.md)
