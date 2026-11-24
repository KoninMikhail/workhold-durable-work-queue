# Local and remote MCP

[Documentation](../README.md) › [Guides](README.md) › **Local and remote MCP**

How to run the optional `workhold-mcp` adapter on stdio or as a remote
Streamable HTTP resource server. OpenAPI
(`openapi/queue.openapi.json`) remains authoritative for HTTP paths and schemas.
`workhold-mcp` calls the sync clients selected at startup and is not part of
the coordinated client version
([ADR 033](../04-architecture/adr/033-optional-local-mcp-adapter.md);
[ADR 034](../04-architecture/adr/034-mcp-tool-exposure-ceiling.md);
[ADR 035](../04-architecture/adr/035-remote-mcp-resource-server.md)).

MCP does not add Workhold authority. MCP does not add exactly-once delivery:
task execution stays at-least-once and fenced by the Workhold API. A tool call
is one HTTP request to Workhold made with the role token you configured.

## Install

```bash
pip install workhold-mcp
```

The console script is `workhold-mcp`. It speaks stdio by default and
Streamable HTTP with `--transport streamable-http`. It is a separate process
from the Workhold runtime.
`workhold-mcp` is not one of `api`, `migrate`, `maintain`, `relay`, or `apply`,
and the runtime image does not contain it. A maintainer must create
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

## Remote Streamable HTTP

`--transport streamable-http` (or `WORKHOLD_MCP_TRANSPORT`) serves the same
registered tools at the single path `/mcp`, as stateless JSON Streamable HTTP.
There is no SSE endpoint. `workhold-mcp` is an OAuth 2.0 resource server only:
it validates bearer JWTs from an external issuer and publishes protected
resource metadata at the well-known path derived from the resource URL. It
does not issue tokens, host a login page, or embed an identity provider. Tokens
must be RS256 or ES256, signed by a key in the configured JWKS, with the
configured issuer, an audience equal to the resource URL, and an expiry.
TLS ends at your reverse proxy.

### Settings

Every setting is a flag or an environment variable. The CLI form and the
environment form of the same setting together fail startup. Any HTTP setting
with stdio fails startup. Startup is closed: a missing or invalid value exits
with exit code 2 before the port opens, and the message names the setting only.

| Flag | Environment | Meaning |
| --- | --- | --- |
| `--transport streamable-http` | `WORKHOLD_MCP_TRANSPORT` | `stdio` (default) or `streamable-http` |
| `--resource-url` | `WORKHOLD_MCP_RESOURCE_URL` | Public URL of the endpoint; the path must end in `/mcp` |
| `--issuer-url` | `WORKHOLD_MCP_ISSUER_URL` | Issuer the tokens must carry |
| `--jwks-url` | `WORKHOLD_MCP_JWKS_URL` | The one JWKS document that holds the signing keys |
| `--allowed-host` | `WORKHOLD_MCP_ALLOWED_HOSTS` | Host allowlist; repeat the flag, or comma-separate the variable |
| `--allowed-origin` | `WORKHOLD_MCP_ALLOWED_ORIGINS` | Origin allowlist for browser clients |
| `--no-browser-origins` | `WORKHOLD_MCP_NO_BROWSER_ORIGINS=true` | no-browser mode: reject every request that carries an `Origin` |
| `--bind-host`, `--bind-port` | `WORKHOLD_MCP_BIND_HOST`, `WORKHOLD_MCP_BIND_PORT` | Listener; defaults are `127.0.0.1` and `8000` |
| `--forwarded-allow-ip` | `WORKHOLD_MCP_FORWARDED_ALLOW_IPS` | Proxy addresses whose forwarded headers are trusted |
| `--clock-skew-seconds` | `WORKHOLD_MCP_CLOCK_SKEW_SECONDS` | Token expiry leeway, 0 to 300, default 60 |

The resource URL, issuer URL, JWKS URL, a non-empty Host allowlist, and exactly
one of an Origin allowlist or no-browser mode are required. Choose
`--no-browser-origins` when only non-browser MCP clients connect. Pass both and
startup fails. DNS-rebinding protection is always on and has no off switch.
The Host allowlist and the Origin allowlist are checked before the token is
read, so a rebinding request never reaches token verification. Host entries
are exact, or `name:*` for any port of `name`. Proxy headers are ignored
unless `--forwarded-allow-ip` lists the proxy address.

The role tokens, the Workhold origins, the preset or allowlist, the optional
content key, and the timeouts keep their meaning from the sections above.

### Example

Placeholders only: `https://mcp.example.invalid/mcp` is the public resource
URL and `https://idp.example.invalid` is your issuer. Export the Workhold
origins and the observer role token as for stdio, then start the process
behind your reverse proxy:

```bash
workhold-mcp \
  --transport streamable-http \
  --resource-url https://mcp.example.invalid/mcp \
  --issuer-url https://idp.example.invalid \
  --jwks-url https://idp.example.invalid/jwks \
  --allowed-host mcp.example.invalid \
  --no-browser-origins \
  --bind-host 127.0.0.1 \
  --bind-port 8000
```

A client sends `Authorization: Bearer <access token from your issuer>` to the
resource URL. The process listens on `127.0.0.1` here, so only a proxy on the
same host reaches it; add `--forwarded-allow-ip` for that proxy's address
when it sets forwarded headers. Never bind a public interface without a proxy
that terminates TLS.

### Scopes

The caller scope is read from the `scope` or `scp` claim. If both appear they
must carry the same set, or the token is rejected.

| Scope | Tools it exposes |
| --- | --- |
| `workhold-mcp:read` | Observer tools (`observer_*`) |
| `workhold-mcp:produce` | Producer tools (`producer_*`) |
| `workhold-mcp:operate` | Routine admin tools (`admin_*`) |

The scopes are separate groups. A token with only `workhold-mcp:read` lists and
calls only observer tools that the startup policy selected. A tool the policy
left out is not listed for any scope.

### Downstream service principals

The caller token proves who may use MCP. It is never forwarded to Workhold, and
MCP caller OAuth is not a service principal. Workhold sees only the role tokens
in the process environment (`WORKHOLD_MCP_OBSERVER_TOKEN`,
`WORKHOLD_MCP_PRODUCER_TOKEN`, `WORKHOLD_MCP_ADMIN_TOKEN`). Each must be a
credential of a Workhold service principal with the matching role and the exact
queue scopes you intend
([security contract](../05-operations/01-security.md#service-principals)).
Every caller with the same MCP scope shares those principals, so run one process
per trust boundary and issue the narrowest role token.

### Local loopback smoke

`tests/sdk/mcp/test_remote_smoke.py` replays the command above on `127.0.0.1`:
it serves a JWKS document and a scripted Workhold listener on ephemeral loopback
ports, starts `python -m workhold_mcp` with the published flags, lists the
tools, and calls `observer_get_capabilities`.

```bash
uv run pytest tests/sdk/mcp/test_remote_smoke.py -q
```

The signing key in that test is a throwaway test-only fixture. It is not an
identity provider and it is not part of the product.

## Authority

Three authorization layers apply to every call. Each one can only narrow the
one before it.

| Layer | Decides | Where it lives |
| --- | --- | --- |
| 1. The startup tool policy | Which tools exist: the selected tools | `--preset`, `--allow-tool`, fixed for the process lifetime |
| 2. The MCP caller scope | Which of those tools this caller sees and may call (remote only) | OAuth scope on the bearer token |
| 3. The Workhold role and queue scope | Whether Workhold accepts the request | The role token in the process environment, checked against its queue scope by Workhold |

Selecting a tool does not grant the role. The role token is the credential that
Workhold sees. Workhold still authorizes that token and the named queue, which
is the Workhold queue scope. On stdio only layers 1 and 3 apply. The MCP caller
scope never adds a tool the startup policy left out, and it is never a Workhold
credential.

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
