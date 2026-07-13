# ADR-007: One Kafka Wire Contract — CloudEvents Envelope, JSON Schema, `chip.<domain>.<source>.<entity>.v<major>` Topics

- **Status:** Accepted (2026-07-10 — reconciliation decision; resolves a cross-subsystem contract conflict found in architecture review)
- **Context:** Subsystem 01 §3 declared itself "the contract every other subsystem builds against" and specified a **CloudEvents 1.0** envelope, JSON-Schema serialization, and topic naming `chip.<domain>.<source>.<entity>.v<major>`. But the ingestion doc (02 §1.6) defined its **own** flat envelope (`schema_version`/`provenance`/`record_key`/`occurred_at`/`payload`) and `chip.raw.<source>` topics, and the NLP doc (03 §6.1) used a third scheme (`raw.news.dawn`, `enriched.*`). Three envelopes and three topic conventions cannot coexist on one bus.

## Decision

**Subsystem 01 §3 is the authoritative wire contract. All producers/consumers conform.**

1. **Envelope: CloudEvents 1.0, structured-content JSON.** The source-native record goes in `data`; the provenance block (ADR-005) lives at `data.provenance` (`source`, `bronze_uri`/`raw_object_key`, `retrieved_at`, `transform_version`). CloudEvents extension attributes `chip_epiweek`, `chip_pcode`, `chip_traceid` are duplicated at the envelope level for routing without deserializing `data`.
2. **Serialization: JSON Schema**, registry-governed (ADR-009), not Avro/Protobuf. Rationale is unchanged from 01 §3.2 (tiny volume, Python-first rotating team, directly inspectable in `kafka-console-consumer`/MinIO/logs).
3. **Topic naming: `chip.<domain>.<source>.<entity>.v<major>`**, `<domain> ∈ {health, weather, hazard, media, gazetteer}`. A breaking schema change is a **new topic** (major bump), enabling blue/green cutover. Concrete names as in 01 §3.4 (e.g. `chip.health.nih_idsr.disease_case_report.v1`, `chip.media.dawn.article_raw.v1`, `chip.media.enriched.media_signal.v1`).
4. **Keys/partitioning/retention/DLQ** as in 01 §3.4–3.6 (structured sources keyed by `pcode`; media raw keyed by `article_url_hash`; one DLQ per source topic).

The ingestion SDK's `Provenance` dataclass (02 §1.3) is retained as the *in-process* representation; it is serialized **into the CloudEvents `data.provenance`** at produce time, not emitted as a competing top-level envelope.

## Alternatives rejected

- **02's flat custom envelope (`schema_version`/`record_key`/…):** minimal and readable, but it reinvents a subset of CloudEvents while losing the standard's tracing/extension attributes and ecosystem tooling; two envelope standards on one bus is exactly the entropy ADR-001/007 exist to prevent.
- **`chip.raw.<source>` (medallion-zone-in-topic) naming:** groups by pipeline zone, but does not encode domain/entity/schema-major, so a breaking schema change can't be expressed as a clean new topic, and cross-domain routing needs payload inspection.
- **`raw.news.*` short names (03):** convenient for a Spark `subscribe` wildcard, but unversioned and domain-implicit; the `chip.media.<outlet>.article_raw.v1` form is still wildcard-subscribable (`chip.media.*.article_raw.v1`) while staying versioned.
- **Avro/Protobuf:** compact and strongly typed, but binary and non-debuggable; unjustified at low-thousands msgs/day (already rejected in 01 §3.2).

## Consequences

- Supersedes 02 §1.6 (envelope + `chip.raw.*` naming) and 03 §6.1 topic names; those docs' *logic* is unchanged, only the wire format/topic strings.
- `libs/chip_schemas` holds the CloudEvents-wrapped JSON Schemas; the SDK's `envelope()` helper (02 §1.3) produces CloudEvents.
- News enrichment subscribes with `chip.media.*.article_raw.v1`; enriched output is `chip.media.enriched.media_signal.v1`.
