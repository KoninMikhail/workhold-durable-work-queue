# Local MCP observer

[Documentation](../README.md) › [Guides](README.md) › **Local MCP observer**

How to run the optional local stdio adapter. OpenAPI
(`openapi/queue.openapi.json`) remains authoritative for HTTP paths and schemas.
`workhold-mcp` calls the sync clients selected at startup and is not part of
the coordinated client version
([ADR 033](../04-architecture/adr/033-optional-local-mcp-adapter.md);
[ADR 034](../04-architecture/adr/034-mcp-tool-exposure-ceiling.md)).

## Install

```bash
pip install workhold-mcp
```

The console script is `workhold-mcp`. It speaks stdio. A maintainer must create
the PyPI project `workhold-mcp` and the `pypi-workhold-mcp` environment before
the first real upload.

On the release workflow, `publish_existing=all` republishes the server and the
coordinated clients. `mcp` is the dispatch value for this package.

## Environment

Origins, role tokens, and the optional content key come from the process
environment. The selected tools decide which of them are required.

| Variable | Purpose |
| --- | --- |
| `WORKHOLD_MCP_PUBLIC_BASE_URL` | Public HTTP origin |
| `WORKHOLD_MCP_ADMIN_BASE_URL` | Admin HTTP origin |
| `WORKHOLD_MCP_OBSERVER_TOKEN` | Observer credential, kept in the environment |
| `WORKHOLD_MCP_PRODUCER_TOKEN` | Producer credential, kept in the environment |
| `WORKHOLD_MCP_ADMIN_TOKEN` | Admin credential, kept in the environment |

The three token variables are not interchangeable. The adapter builds only the
clients the selected tools need and does not borrow one role token for another
role. The observer preset requires `WORKHOLD_MCP_PUBLIC_BASE_URL`,
`WORKHOLD_MCP_ADMIN_BASE_URL`, and `WORKHOLD_MCP_OBSERVER_TOKEN`.

Optional timeouts are `WORKHOLD_MCP_CONNECT_TIMEOUT_S` (default 5 seconds),
`WORKHOLD_MCP_READ_TIMEOUT_S` (default 30 seconds), and
`WORKHOLD_MCP_TOTAL_TIMEOUT_S` (omit it for no total cap). Each timeout must be
a positive number. `WORKHOLD_MCP_VERIFY_TLS` is `true` unless set to `false`.

`WORKHOLD_MCP_CONTENT_KEY` is accepted only when a producer tool is selected.
The content key is a `ProducerClient` constructor secret.
The content key is not a role credential, and observer reads of encrypted payloads stay sealed.
That means observer reads of keyed payloads stay sealed.
An empty key fails startup through the producer client.
A key with no producer tool selected fails startup.

## Policy

Preset ids are `observer`, `producer`, `operator`, and `routine`.
Omitted policy selects `observer`.
Role presets are not cumulative.
`routine` is the union of `observer`, `producer`, and `operator`.

`observer` is the read-only observer tools. `producer` is the producer tools.
`operator` is the routine admin tools. An explicit preset or allowlist replaces
the omitted default for the process lifetime.

A custom allowlist is an exact, case-sensitive set of stable tool names, such
as `observer_list_dead_letters` or `producer_enqueue`. Pass repeatable
`--allow-tool` or a comma-separated `WORKHOLD_MCP_ALLOWED_TOOLS`. Unknown,
empty, and duplicate names fail startup. Preset ids, wildcards, and operation
ids are not tool names.

`--preset` and `--allow-tool` conflict with `WORKHOLD_MCP_PRESET` and
`WORKHOLD_MCP_ALLOWED_TOOLS`. CLI and env together fail startup. The two CLI
forms together fail startup, and the two environment variables together fail
startup. The process exits with exit code 2 before stdio opens. The message
names the setting or flag and does not include the supplied value.

## Start on the host

Export the variables in the host process. With no policy arguments the script
uses the observer preset. Credentials are not command-line arguments and must
not be passed as tool arguments.

```bash
export WORKHOLD_MCP_PUBLIC_BASE_URL="http://127.0.0.1:8080"
export WORKHOLD_MCP_ADMIN_BASE_URL="http://127.0.0.1:8080"
export WORKHOLD_MCP_OBSERVER_TOKEN="WORKHOLD_MCP_OBSERVER_TOKEN"
workhold-mcp
```

A named preset uses the same environment for credentials:

```bash
export WORKHOLD_MCP_PUBLIC_BASE_URL="http://127.0.0.1:8080"
export WORKHOLD_MCP_PRODUCER_TOKEN="WORKHOLD_MCP_PRODUCER_TOKEN"
workhold-mcp --preset producer
```

Logs go to stderr. Invalid configuration, including an unknown argument, prints
a setting name or a generic rejection to stderr and exits with exit code 2
before the server runs. The message does not include the supplied value.

## Authority

Three layers apply to every call: selected tools, the role token, and the
Workhold queue scope. Selecting a tool does not grant the role. Workhold still
authorizes the token and the named queue.

Bulk execute needs the preview object returned by the matching preview tool.
Execute needs the preview object. The adapter does not cache a preview, accept
a bare confirmation token, or auto-preview.

The catalog has no claim, heartbeat, complete, fail, or break-glass tool.
Worker lease tools are absent from every preset and from the custom allowlist.

## Tools

The adapter registers only the tools selected at startup. Discovery order is
the sorted stable name, and that set stays fixed for the process lifetime.
Each list call returns one page. Time filters are timezone-aware RFC 3339
timestamps.

The observer preset registers these tools:

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
