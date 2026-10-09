# 021. CloudEvents 1.0 delivery envelope

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Delivery Outbox event format and HTTP adapter

## Context

A shared Delivery Outbox needs stable event identity and routing metadata without
inventing an application-specific envelope. An opaque payload alone is not enough
for deduplication, observability, and future transport adapters.

## Decision

Delivery events use CloudEvents 1.0 JSON in structured content mode. queue-service
assigns stable `id`, `time`, and `specversion`; the application supplies
`source`, `type`, optional `subject`, `datacontenttype`, and `data`. HTTP sends
`application/cloudevents+json`.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Custom minimal envelope | Creates another platform-specific standard and SDK mapping |
| Opaque payload only | No stable metadata for routing, deduplication, and observability |
| CloudEvents binary mode first | More HTTP-header coupling and harder generic persistence |

## Consequences

**Positive:** Standard tooling, stable event IDs, and transport-neutral
semantics.

**Negative / trade-offs:** Applications must supply a valid source and type;
extensions need an allowlist and size limits.

**Follow-up:** Phase 5 OpenAPI and schema lock validation, the extension allowlist,
and adapter mapping.

## References

- [CloudEvents 1.0 specification](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)
- [HTTP protocol binding](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/http-protocol-binding.md)
