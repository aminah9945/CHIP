# ADR-009: Schema Registry — Apicurio (Postgres-Backed)

- **Status:** Accepted (2026-07-10 — reconciliation decision; resolves 01-vs-07 conflict)
- **Context:** Subsystem 01 §3.3 chose **Apicurio Registry** (Apache-2.0, Postgres-backed) with Karapace named as a drop-in alternative. The infrastructure doc (07 §5.1, §6.2, hardware table) instead assumed **Karapace** throughout and flagged the mismatch as its OQ-6. Backups, sizing, and the "rebuildable state" story differ between the two, so the pick must be singular.

## Decision

**Apicurio Registry, Postgres-backed**, is the schema registry.

1. It is **Apache-2.0** (no Confluent Community License ambiguity) and persists schemas in **PostgreSQL** — the engine ADR-003 already mandates — so it adds a schema table set to an existing backup surface rather than a new stateful engine.
2. Confluent-compatible REST API, so standard Kafka SerDes and the JSON-Schema subjects (ADR-007) work unchanged.
3. **Compatibility policy per subject: default BACKWARD** (01 §6.2); a schema PR runs Apicurio's compatibility check in CI (red = blocked).
4. **Backup:** covered by the normal Postgres backup (pgBackRest, 07 §6.2) — **no separate compacted-topic export job is needed** (that step existed only for the Karapace assumption and is removed).

## Alternatives rejected

- **Karapace (Aiven, Apache-2.0):** 1:1 Confluent-API compatible and tiny, but it stores schemas in a **compacted Kafka topic** — i.e. new durable state on the bus that must be separately exported to MinIO nightly to be rebuildable. Apicurio folds that state into Postgres, which we already back up continuously. Fewer moving parts wins.
- **Confluent Schema Registry:** the reference implementation and legally usable for self-hosted internal use, but it carries the Confluent Community License and telemetry; we keep the stack Apache-2.0 and telemetry-free.

## Consequences

- Supersedes 07's Karapace references (§5.1 schema-registry line, §6.2 "Karapace schemas → nightly JSON export", and the hardware-sizing "Kafka … + Karapace" row → "+ Apicurio"). Apicurio can run in the same container group; its state is a `schema_*` table set in the main Postgres.
- One backup mechanism (Postgres) now also covers the registry — consistent with 07's principle 4 ("fewest stateful services").
- 07 OQ-6 is closed.
