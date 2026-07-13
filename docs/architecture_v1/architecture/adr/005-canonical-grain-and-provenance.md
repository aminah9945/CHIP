# ADR-005: Canonical Grain (District × Epi-Week) and Mandatory Provenance

- **Status:** Accepted (2026-07-10 — follows directly from proposal commitments)
- **Context:** The proposal commits to district-level analytics (the operationally relevant resolution for surveillance in Pakistan) and to explainable, auditable outputs for policy trust. Heterogeneous sources arrive with inconsistent place names (English/Urdu/transliterations) and time conventions.

## Decision

1. **Canonical spatial key:** Pakistan district **P-codes** from a versioned canonical gazetteer (single shared source of truth, with alias tables for Urdu/English/transliteration variants). Every fact row and every KG Location node carries a P-code.
2. **Canonical temporal key:** **epidemiological week** matching NIH IDSR's convention (verified in the data-model subsystem doc), via a shared `libs/epiweek` Python library. Sub-weekly data keeps native timestamps but always maps to an epi-week for analytical joins.
3. **Mandatory provenance on every derived record:** `source_id`, `retrieved_at`, `raw_object_uri` (MinIO), `transform_version`. Kafka envelopes and all silver/gold tables enforce these fields. KG edges reify evidence (edge → Evidence node → source document + offsets + confidence + model version).

## Consequences

- The gazetteer and epi-week libraries are **Phase-0 deliverables** — nothing downstream can be built correctly without them.
- Any output (alert, forecast, RAG summary) can be traced to source documents — the auditability requirement is structural, not procedural.
- Re-running improved models bumps `transform_version`, enabling clean re-enrichment via Kafka/bronze replay (ties to ADR-002).
