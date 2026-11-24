# Codebase map

Product: **Workhold - Durable work queue**. Runtime package: `workhold`.

```
.
├── src/workhold/          # import package (not stdlib queue)
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
│   ├── workhold-client-core/   # _workhold_client_core + workhold_client_testing
│   ├── workhold-producer/      # workhold_producer (+ bridge)
│   ├── workhold-consumer/      # workhold_consumer
│   ├── workhold-admin/         # workhold_admin
│   ├── workhold-mcp/           # workhold_mcp
│   └── client-operation-ownership.json
├── alembic/                    # migrations
├── openapi/                    # queue.openapi.json
├── tests/                      # pytest (unit/integration/conformance/chaos/sdk)
├── benchmarks/                 # qualification workloads
├── tools/                      # ownership checker, client release gate, version sync
├── docs/                       # 00–08 + _ai Memory Bank
│   └── 04-architecture/schemas/# JSON Schema companions (incl. named-queue-catalog)
├── website/                    # GitHub Pages: landing.md, MkDocs hooks.py, theme overrides
├── mkdocs.yml                  # site config; docs_dir = docs/, excludes _ai/
├── graphify-out/               # local knowledge graph, gitignored
├── Dockerfile
├── docker/entrypoint.sh        # *_FILE → NAME then exec workhold
├── docker-compose.dev.yml      # profiles maintain|relay|apply; see 02-deployment.md
├── .github/                    # workflows, issue and pull request templates, CODEOWNERS
├── CONTRIBUTING.md             # pull requests, commits, checks
├── SECURITY.md                 # private vulnerability reports
├── CODE_OF_CONDUCT.md
├── SUPPORT.md
├── release-please-config.json  # server, client, and workhold-mcp trains (ADR 031, ADR 033)
├── release-packages.json       # client publish order and version files
└── CHANGELOG.md                # written by release-please
```

For a typical new module, look at the neighboring package under `src/workhold/`
(for example `intake/` for enqueue, `delivery/` for relay). Client APIs are
role packages under `packages/workhold-{producer,consumer,admin}/`
(shared core: `_workhold_client_core`). The `queue-client` /
`queue_service_client` prototype has been removed.

Break-glass: delivery ops `forceDeliveryReclaim` / `forceDeliveryDeadLetter` in
`api/admin_break_glass.py` + `operations/break_glass.py`; durable elevation
table `break_glass_elevations` (`storage/models.py` → `BreakGlassElevation`).
Ops catalog: [07-admin-tools.md](../05-operations/07-admin-tools.md#break-glass-operations).

Client docs: [06-client-sdk-ergonomics.md](../02-guides/06-client-sdk-ergonomics.md).
Optional local observer adapter: [07-local-mcp-observer.md](../02-guides/07-local-mcp-observer.md)
([ADR 033](../04-architecture/adr/033-optional-local-mcp-adapter.md)).
Release gate: `tools/client_release_gate.py` (wheels + `uv publish --dry-run`).

Documentation site: `mkdocs.yml` publishes `docs/` unchanged. `website/hooks.py`
builds the navigation from the numbered sections, serves `website/landing.md` at
`/`, moves `docs/README.md` to `/docs/`, and rewrites links that leave `docs/` to
GitHub. `.github/workflows/pages.yml` builds with `--strict` on pull requests and
deploys to GitHub Pages from `main`.
