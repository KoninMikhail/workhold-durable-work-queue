# 034. MCP tool exposure ceiling

**Status:** Accepted  
**Date:** 2026-11-24  
**Scope:** Local `workhold-mcp` catalog and startup policy

## Context

The local stdio adapter can advertise producer and routine admin tools. A
policy that granted a Workhold role, or one shared token, would collapse the
separate security planes in [ADR 013](013-separate-security-planes.md).

## Decision

The MCP catalog is an exposure ceiling over the sync `ObserverClient`,
`ProducerClient`, and `AdminClient` surfaces. Startup policy only narrows
that ceiling. A selected tool still uses that role's token. Worker lease
tools and break-glass tools are not catalog entries.

## Alternatives considered

| Option | Why it was not chosen |
| --- | --- |
| Policy grants roles | A preset would mint authority the token does not have |
| One shared token | Collapses the producer, observer, and admin planes |
| operationId tool names | Binds the catalog to HTTP names instead of stable methods |
| Worker tools | Exposes claim, heartbeat, complete, and fail |
| `getattr` dispatch | Calls any client attribute a model can name |

## Consequences

**Positive:** A preset or allowlist selects tools without granting a role or a break-glass path.

**Negative / trade-offs:** Each selected role needs its own token. Unselected names stay hidden for the process lifetime.

## References

- [ADR 013](013-separate-security-planes.md)
- [ADR 033](033-optional-local-mcp-adapter.md)
- [Local MCP observer](../../02-guides/07-local-mcp-observer.md)
