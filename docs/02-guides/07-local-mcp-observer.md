# Local MCP observer

[Documentation](../README.md) › [Guides](README.md) › **Local MCP observer**

How to run the optional local stdio adapter. OpenAPI
(`openapi/queue.openapi.json`) remains authoritative for HTTP paths and schemas.
`workhold-mcp` calls `ObserverClient` and is not part of the coordinated client
version
([ADR 033](../04-architecture/adr/033-optional-local-mcp-adapter.md)).

## Install

```bash
pip install workhold-mcp
```

The console script is `workhold-mcp`. It speaks stdio and reads settings only
from the process environment. A maintainer must create the PyPI project
`workhold-mcp` and the `pypi-workhold-mcp` environment before the first real
upload.

On the release workflow, `publish_existing=all` republishes the server and the
coordinated clients. `mcp` is the dispatch value for this package.

## Environment

Three variables are required:

| Variable | Purpose |
| --- | --- |
| `WORKHOLD_MCP_PUBLIC_BASE_URL` | Public HTTP origin |
| `WORKHOLD_MCP_ADMIN_BASE_URL` | Admin HTTP origin |
| `WORKHOLD_MCP_OBSERVER_TOKEN` | Observer credential, kept in the environment |

Optional timeouts are `WORKHOLD_MCP_CONNECT_TIMEOUT_S` (default 5 seconds),
`WORKHOLD_MCP_READ_TIMEOUT_S` (default 30 seconds), and
`WORKHOLD_MCP_TOTAL_TIMEOUT_S` (omit it for no total cap). Each timeout must be
a positive number. `WORKHOLD_MCP_VERIFY_TLS` is `true` unless set to `false`.

## Start on the host

Export the variables in the host process, then run the script with no
arguments. The credential is not a command-line argument and must not be passed as a tool argument.

```bash
export WORKHOLD_MCP_PUBLIC_BASE_URL="http://127.0.0.1:8080"
export WORKHOLD_MCP_ADMIN_BASE_URL="http://127.0.0.1:8080"
export WORKHOLD_MCP_OBSERVER_TOKEN="WORKHOLD_MCP_OBSERVER_TOKEN"
workhold-mcp
```

Logs go to stderr. Invalid configuration, including any extra argument, prints
a setting name or a generic rejection to stderr and exits with exit code 2
before the server runs. The message does not include the supplied value.

## Tools

The adapter registers tools only. It does not register producer tools or
break-glass tools. Each list call returns one page. Time filters are
timezone-aware RFC 3339 timestamps.

| Tool | Call |
| --- | --- |
| `observer_get_capabilities` | Deployment capabilities |
| `observer_get_task` | One task |
| `observer_list_task_attempts` | One page of attempts for one task |
| `observer_get_queue` | One queue |
| `observer_get_stats` | Statistics snapshot |
| `observer_get_maintenance_status` | Maintenance status |
| `observer_list_inspection_tasks` | One page of tasks in one queue |
| `observer_list_inspection_attempts` | One page of attempts in a time range |
| `observer_list_dead_letters` | One page of dead letters in a time range |

A payload returned by an observer tool is the client value. Errors are
redacted: they omit credentials and exception text.

---

← [Client SDK](06-client-sdk-ergonomics.md) · [Guides](README.md)
