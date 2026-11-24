# 035. Remote MCP resource server

**Status:** Accepted  
**Date:** 2026-11-24  
**Scope:** `workhold-mcp` Streamable HTTP transport, deployment and authorization

## Context

[ADR 033](033-optional-local-mcp-adapter.md) ships `workhold-mcp` as a local stdio adapter. Operators also need to reach it from a remote MCP client without handing that client a Workhold credential or collapsing the security planes of [ADR 013](013-separate-security-planes.md).

## Decision

- `workhold-mcp` serves Streamable HTTP at `/mcp` as an OAuth 2.0 resource server. An external issuer signs the bearer JWT. The process checks it against one configured JWKS, issuer and resource audience.
- It is a separate process. It is not one of `api`, `migrate`, `maintain`, `relay`, or `apply`, and it is not in the runtime image of [ADR 015](015-single-image-multi-role.md).
- Three authorization layers apply: the startup tool policy, the MCP caller scope, and the Workhold role and queue scope. Scopes are `workhold-mcp:read`, `workhold-mcp:produce`, and `workhold-mcp:operate`. They narrow the registered tools and never add one.
- A Host allowlist is required. An Origin allowlist or no-browser mode is required. Both run before token verification, and DNS-rebinding protection has no off switch.
- The process calls Workhold with its own role tokens. The caller token is not forwarded and OAuth is not a Workhold service principal. MCP does not add Workhold authority and MCP does not add exactly-once.
- Public examples use `https://mcp.example.invalid/mcp` and `https://idp.example.invalid`. The loopback key fixture in the smoke test is test-only.

## Alternatives considered

| Option | Why it was not chosen |
| --- | --- |
| SSE transport | A second long-lived channel with no gain over stateless JSON |
| An embedded IdP | Makes Workhold an identity provider and widens the attack surface |
| MCP inside the runtime image | Adds an HTTP surface and dependency to every role of ADR 015 |
| Forwarding the caller token | Collapses the MCP caller and the Workhold principal |
| Disabling rebinding protection | Lets a browser page reach a loopback or private listener |

## Consequences

**Positive:** A remote client gets only the tools both the policy and its scope allow, and Workhold authority stays with the role tokens.

**Negative / trade-offs:** The operator runs an external issuer and one process per trust boundary. Callers with the same scope share that process's principals.

## References

- [ADR 013](013-separate-security-planes.md), [ADR 015](015-single-image-multi-role.md), [ADR 033](033-optional-local-mcp-adapter.md), [ADR 034](034-mcp-tool-exposure-ceiling.md)
- [Local and remote MCP](../../02-guides/07-local-mcp-observer.md)
