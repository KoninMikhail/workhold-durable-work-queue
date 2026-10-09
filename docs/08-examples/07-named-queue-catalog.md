# Named-queue catalog: mount + queue apply

[Documentation](../README.md) › [Examples](README.md) › **Catalog apply**

**Who:** the application deploy operator. The catalog is JSON with a list of named queues,
not a namespace/tenant resource.

**What it does:** one-shot `queue apply` (image command `["apply"]` under
`ENTRYPOINT ["/app/entrypoint.sh"]`) creates missing named queues with
`initial_policy` through the same audited create path as admin HTTP. This is
**ensure-exists**, not a GitOps reconcile of live state: named queues that already exist
(any state/policy) are skipped; names removed from the file are **not** deleted
and are **not** drained.

**What is absent:** the catalog is **not baked into the image** (no `Dockerfile` `COPY`);
`api` startup and `/readyz` do not read the catalog; pause/drain/activate policy —
admin HTTP only.

Example file: [named-queue-catalog.example.json](named-queue-catalog.example.json)
(schema: [named-queue-catalog.schema.json](../04-architecture/schemas/named-queue-catalog.schema.json)).

## 1. Mount the catalog

Set an absolute `QUEUE_CATALOG_PATH` to a path the process under uid
`10001` can read (Compose volume / Kubernetes ConfigMap). The catalog is not a
`*_FILE` secret. There is no `QUEUE_CATALOG_PATH_FILE` in the entrypoint.

## 2. Run apply after migrate

```bash
docker compose -f docker-compose.dev.yml --profile apply run --rm apply
```

In Kubernetes — a docs-only Job with a volume from a ConfigMap, after the migrate Job, command
`["apply"]`. The `api` service does **not** `depends_on` apply.

## 3. Partial apply

If a create in the middle of the catalog fails after validation, earlier creates
may already have committed. Repeat `queue apply` — ensure-exists is safe
(D-19).

Admin steps after bootstrap: [04-admin-queues.md](../02-guides/04-admin-queues.md).
Deploy: [02-deployment.md](../05-operations/02-deployment.md).
