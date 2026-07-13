# ADR-003: Postgres-Centric Storage + Neo4j + MinIO

- **Status:** Proposed (2026-07-10, Claude recommendation — awaiting explicit confirmation from project lead; default in effect for subsystem designs)
- **Context:** CHIP needs six storage capabilities: raw immutable documents (auditability), geospatial (district boundaries), time series (weather/cases), vector embeddings (RAG), a knowledge graph (CHKG — headline deliverable), and full-text search over news. Every additional database engine is another system a rotating student team must upgrade, back up, and monitor for 3+ years. Total data over the project's life is estimated < 100 GB (excluding raw HTML/PDF archive).

## Decision

Three systems, total:

1. **PostgreSQL** — the analytical core, with extensions:
   - **PostGIS** — district geometries, spatial joins
   - **TimescaleDB** — weather/case/media-signal time series (hypertables)
   - **pgvector** — embeddings for graph-RAG retrieval
   - Postgres native FTS for full-text search initially
2. **MinIO** — S3-compatible object store for the bronze zone: raw immutable PDFs, HTML, API payloads, with versioning; every derived record points back here (provenance).
3. **Neo4j (Community)** — the Climate–Health Knowledge Graph, with evidence reification; export path to PyTorch Geometric for GNN work.

Cross-domain analysis (weather × cases × media signals) happens as SQL joins on the conformed district × epi-week grain — one query, one database.

## Alternatives rejected

- **Lakehouse-first (Delta/Iceberg on MinIO as source of truth):** adds table format + catalog + forces Spark into every transformation, contradicting ADR-002; pays off at ~1000× CHIP's data volume.
- **Polyglot best-of-breed (Timescale + OpenSearch + Qdrant + Neo4j + MinIO):** five engines to babysit; splits data so cross-domain joins move into application code — directly against the project's core scientific need.

## Consequences

- One backup/restore strategy for the analytical core; one connection string for students.
- If Urdu full-text search over millions of articles becomes central and Postgres FTS falls short, revisit with a scoped OpenSearch addition (recorded as the known escape hatch).
- Extension version compatibility (PostGIS/Timescale/pgvector) must be pinned in the container image.
