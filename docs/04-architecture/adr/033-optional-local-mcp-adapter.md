# 033. Optional local MCP adapter

**Status:** Accepted  
**Date:** 2026-11-23  
**Scope:** Local stdio adapter `workhold-mcp` and its third release train

## Context

Operators want read-only observer tools on a local host. HTTP and OpenAPI stay
the normative contract. Folding MCP into the server image or the coordinated
client version would mix that adapter into those release trains.

## Decision

`workhold-mcp` is an optional local stdio adapter over `ObserverClient`. It
holds no Workhold runtime role and publishes on a third release train, apart
from the server image and the coordinated client version. Phase 4 presets and
Phase 5 remote HTTP are later work.

## Alternatives considered

| Option | Why it was not chosen |
| --- | --- |
| MCP inside `workhold` | Couples a local adapter to the server image and its roles |
| MCP inside the coordinated client version | Ships the adapter with every client release |
| Hand-rolled JSON-RPC | Rebuilds the stdio session the adapter already speaks |

## Consequences

**Positive:** A host runs observer tools without installing them into the server or the client set.

**Negative / trade-offs:** Publication is a third train. `all` republishes the server and the coordinated clients; `mcp` publishes this package.

**Follow-up:** A maintainer creates the PyPI project `workhold-mcp` and the `pypi-workhold-mcp` environment before the first real upload.

## References

- [ADR 029](029-role-split-python-clients.md)
- [ADR 030](030-workhold-distribution-names.md)
- [ADR 031](031-separate-server-and-client-release-trains.md)
- [Local MCP observer](../../02-guides/07-local-mcp-observer.md)
- [Client protocol](../09-client-protocol.md)
