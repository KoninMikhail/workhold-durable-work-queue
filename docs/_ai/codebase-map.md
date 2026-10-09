# Codebase map

Product: **Workhold - Durable work queue**. Runtime package: `queue_service`.

```
.
├── src/queue_service/          # import package (not stdlib queue)
│   ├── admission/              # hard ceilings / depth gates
│   ├── api/                    # pure ASGI apps, v1 + admin routes
│   ├── application/            # lease, inspection, expiry services
│   ├── delivery/               # Delivery Outbox relay + CloudEvents
│   ├── domain/                 # queue control + catalog parse (catalog.py)
│   ├── infrastructure/         # PostgreSQL repositories
│   ├── intake/                 # enqueue service + UoW
│   ├── maintenance/            # partitions / retention
│   ├── observability/          # metrics, correlation, retention alerts
│   ├── operations/             # DLQ, bulk, break-glass, routine
│   ├── roles/                  # api|migrate|maintain|relay|apply entrypoints
│   ├── security/               # principals, redaction, payload policy
│   ├── storage/                # SQLAlchemy models
│   └── db.py                   # Base / metadata
├── packages/
│   ├── queue-service-client-core/   # _queue_service_client_core + queue_service_client_testing
│   ├── queue-service-producer/      # queue_service_producer (+ bridge)
│   ├── queue-service-consumer/      # queue_service_consumer
│   ├── queue-service-admin/         # queue_service_admin
│   └── client-operation-ownership.json
├── alembic/                    # migrations
├── openapi/                    # queue.openapi.json
├── tests/                      # pytest (unit/integration/conformance/chaos/sdk)
├── benchmarks/                 # qualification workloads
├── tools/                      # ownership checker, client release gate, version sync
├── docs/                       # 00–08 + _ai Memory Bank
│   └── 04-architecture/schemas/# JSON Schema companions (incl. named-queue-catalog)
├── graphify-out/               # local knowledge graph, gitignored
├── Dockerfile
├── docker/entrypoint.sh        # *_FILE → NAME then exec queue
├── docker-compose.dev.yml      # profiles maintain|relay|apply; see 02-deployment.md
├── .github/workflows/          # ci.yml and release-please release.yml
├── release-please-config.json  # Conventional Commits → changelog and tags
├── release-packages.json       # client publish order and version files
└── CHANGELOG.md                # written by release-please
```

For a typical new module, look at the neighboring package under `src/queue_service/`
(for example `intake/` for enqueue, `delivery/` for relay). Client APIs are
role packages under `packages/queue-service-{producer,consumer,admin}/`
(shared core: `_queue_service_client_core`). The `queue-client` /
`queue_service_client` prototype has been removed.

Break-glass: delivery ops `forceDeliveryReclaim` / `forceDeliveryDeadLetter` in
`api/admin_break_glass.py` + `operations/break_glass.py`; durable elevation
table `break_glass_elevations` (`storage/models.py` → `BreakGlassElevation`).
Ops catalog: [07-admin-tools.md](../05-operations/07-admin-tools.md#break-glass-operations).

Client docs: [06-client-sdk-ergonomics.md](../02-guides/06-client-sdk-ergonomics.md).
Release gate: `tools/client_release_gate.py` (wheels + `uv publish --dry-run`).
