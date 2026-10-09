# Client SDK: install, sync/async, ergonomics

[Documentation](../README.md) › [Guides](README.md) › **Client SDK**

How to install and use the role-split Python clients. OpenAPI
(`openapi/queue.openapi.json`) remains authoritative for HTTP paths and schemas;
the SDK is a thin adapter. There is **no** `queue-client` / `queue_service_client`
distribution and **no** auth-provider or legacy facade.

## Packages (coordinated version set)

| Distribution | Import | Role |
| --- | --- | --- |
| `queue-service-client-core` | `_queue_service_client_core`, `queue_service_client_testing` | Shared transport / errors / test kit (no operation clients) |
| `queue-service-producer` | `queue_service_producer` | Enqueue, resolve, producer inspect/cancel; optional bridge |
| `queue-service-consumer` | `queue_service_consumer` | Claim / lease ops + `ConsumerSupervisor` |
| `queue-service-admin` | `queue_service_admin` | `ObserverClient`, `AdminClient`, `BreakGlassClient` |

All four share one repository version. Roles pin
`queue-service-client-core>=X.Y.0,<X.(Y+1).0`. Publish is atomic: core first,
then the three roles; partial sets and `queue-client` are rejected by CI.
The release gate derives this interval from the coordinated role version (a
role at `1.2.Z` requires exactly `>=1.2.0,<1.3.0`) and checks both
`project.dependencies` and `project.optional-dependencies.async`. It then checks
the same unconditional and `extra == "async"` `Requires-Dist` entries in every
built role wheel. A syntactically valid stale range from an earlier minor,
wrong lower patch, wrong upper minor, missing/duplicate dependency, or async
drift blocks release with the affected role and metadata section in the
diagnostic. Update base and async declarations together with every coordinated
version bump.

### Sync (default) vs async

```bash
# Sync HTTP (stdlib) — base wheels stay free of httpx
pip install queue-service-producer
pip install queue-service-consumer
pip install queue-service-admin

# Async HTTP (optional extra pulls core[async] → httpx)
pip install "queue-service-producer[async]"
pip install "queue-service-consumer[async]"
pip install "queue-service-admin[async]"

# Producer bridge that needs a concrete Postgres driver
pip install "queue-service-producer[bridge-postgres]"
```

```python
# Sync
from queue_service_producer import HttpJsonTransport, ProducerClient
from queue_service_consumer import ConsumerClient, ConsumerSupervisor
from queue_service_admin import ObserverClient, AdminClient, BreakGlassClient

# Async (requires [async] extra) — import from async_* modules
from queue_service_producer.async_client import AsyncProducerClient
from queue_service_consumer.async_client import AsyncConsumerClient
from queue_service_consumer.async_supervisor import AsyncConsumerSupervisor
from queue_service_admin.async_client import (
    AsyncObserverClient,
    AsyncAdminClient,
    AsyncBreakGlassClient,
)
from _queue_service_client_core.async_transport import HttpxAsyncTransport
```

## Explicit bearer tokens (no auth provider)

Pass role credentials explicitly. The SDK never discovers tokens from the
environment, never mints break-glass JIT, and never mixes roles.

```python
from _queue_service_client_core.config import ClientConfig
from _queue_service_client_core.transport import HttpJsonTransport
from queue_service_producer import ProducerClient

config = ClientConfig.for_public(
    "https://queue.example",
    connect_timeout_s=5.0,
    read_timeout_s=30.0,
    verify_tls=True,  # default; set ca_cert_path for private CAs
)
transport = HttpJsonTransport.from_config(config)
client = ProducerClient(transport, bearer_token=producer_secret)
```

| Client | Listener | Credential |
| --- | --- | --- |
| `ProducerClient` | public `/v1` | PRODUCER |
| `ConsumerClient` | public `/v1` | WORKER |
| `ObserverClient` / `AdminClient` | public + private `/admin/v1` | OBSERVER / ADMIN |
| `BreakGlassClient` | private `/admin/v1` | short-lived BREAK_GLASS JIT |

Do not place admin / break-glass tokens in producer or consumer pods.

## TLS and timeouts

`ClientConfig` (`_queue_service_client_core.config`):

| Field | Default | Notes |
| --- | --- | --- |
| `verify_tls` | `True` | Fail closed unless explicitly disabled for local-only labs |
| `ca_cert_path` / `client_cert_path` / `client_key_path` | `None` | Optional mTLS / private CA |
| `connect_timeout_s` | `5.0` | Positive |
| `read_timeout_s` | `30.0` | Positive; long-poll needs `wait+5` |
| `total_timeout_s` | `None` | When set, must cover long-poll `wait+10` |

Bounded claim long polling (capability `long_polling=true`, max wait 20 s):
client read budget = wait+5, total = wait+10; production reverse-proxy upstream
idle/response timeout ≥ **30 s**. See [08-consumer-long-polling.md](../08-examples/08-consumer-long-polling.md).

## Pagination ceilings

Observer/Admin cursor lists use bounded helpers in
`queue_service_admin.pagination` / `async_pagination`. Callers **must** pass
`max_pages` and/or `max_items` (positive integers). The helpers forward server
cursors only, do not prefetch, and do not retry cursor protocol errors.

```python
from queue_service_admin.pagination import iter_queues

for queue in iter_queues(observer, max_pages=10, max_items=500):
    ...
```

## Retry safety

Retries are **opt-in**. Role clients do not retry by default. Use
`_queue_service_client_core.retry` with an explicit `RetryPolicy` and immutable
`RetryRequest`. Classes:

| Class | Meaning |
| --- | --- |
| `safe-read` | Idempotent reads only |
| `same-idempotency-key` | Same key + same body |
| `same-resource-identity` | Same immutable resource identity/path (no wire idempotency key) |
| `same-terminal-body` | Same terminal fingerprint |
| `never` | Refuse retry helpers entirely |

`retryable` / `retry_after_ms` hints alone never authorize unsafe retries.
Changing key, resource identity, body, or operation class raises
`RetryRequestChangedError`.

## Instrumentation redaction

`SyncInstrumentation` / `AsyncInstrumentation` emit allowlisted, low-cardinality
events. Hooks never receive headers, query values, bodies, payloads, queue/task/
claim identifiers, tokens, idempotency keys, or incident text. Hook failures
cannot alter Queue HTTP behavior. Use `redact_text` / test-kit secret helpers
when asserting in tests.

## Test kit

Public kit ships inside the **core** wheel as `queue_service_client_testing`
(no server, DB, or pytest dependency):

```python
from queue_service_client_testing import (
    ScriptedSyncTransport,
    ScriptedAsyncTransport,
    synthetic_bearer_token,
    scenario_cancellation,
    scenario_pagination,
)
```

Recording transports and the test kit **do not** replace live PostgreSQL
conformance.

## Codecs and capability guards

Opt-in payload codecs (`_queue_service_client_core.codecs`) keep opaque JSON on
the wire. Decode failures expose codec/type metadata and preserve raw for
recovery — they do not invent server fields.

Capability guards fail closed before HTTP:

- `require_long_polling(capabilities, wait_seconds)` for `wait_seconds > 0`
- `require_batch_claim(capabilities, max_tasks)` for `max_tasks > 1`
- `require_delivery_events(capabilities)` when emitting `events[]`

MVP keeps `batch_claim=false` / `max_tasks=1`. Do not invent query params or
auth-provider paths around these gates.

## Verification (maintainers)

```bash
uv lock --check
uv run python tools/check_client_operation_ownership.py
uv run pytest tests/sdk tests/conformance/test_client_operation_coverage.py \
  tests/conformance/test_client_matrix.py -q
uv run python tools/client_release_gate.py
```

Immutable qualification evidence:
[09-client-release-qualification.md](../05-operations/09-client-release-qualification.md).

## Next

- Producer: [02-producer-enqueue.md](02-producer-enqueue.md)
- Consumer: [03-worker-claim-complete.md](03-worker-claim-complete.md)
- Admin: [04-admin-operations.md](04-admin-operations.md)
- Protocol: [09-client-protocol.md](../04-architecture/09-client-protocol.md)
- ADR: [029-role-split-python-clients.md](../04-architecture/adr/029-role-split-python-clients.md)
