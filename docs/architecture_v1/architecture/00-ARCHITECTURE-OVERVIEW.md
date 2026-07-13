# CHIP — Architecture Overview (Macro Plan)

**Climate–Health Intelligence Platform · PCN Research Group, NUCES/FAST Islamabad · NRPU**
*Status: living document. Created 2026-07-10. Decisions live in `adr/`; subsystem detail lives in `subsystems/`.*

---

## 1. What CHIP is, architecturally

CHIP is a **data-integration and intelligence platform**, not a transactional product. Four heterogeneous
source families arrive at very different cadences, get harmonized onto one canonical grain
(**district × epidemiological week**), enriched by NLP, linked into a knowledge graph, modeled for risk,
and served to institutional stakeholders (NIH, NDMA/PDMAs, MoCC) through a dashboard with
explainable, evidence-backed alerts.

**Design premise (drives everything):** CHIP is *not* a big-data problem — it is a
**hard-integration problem**. ~160 districts, weekly bulletins, and a *relevance-filtered* news
feed; **single-digit TB over the project's life** (see §1.1). The genuinely hard parts are
Urdu/English NLP quality, entity-linking messy place names to districts, epi-week alignment and lag
modeling, evidence provenance for policy trust, and surviving schema drift when institutional feeds
arrive. The genuinely dangerous risks are **team turnover and operational entropy**, not throughput.
Every decision below optimizes for correctness, auditability, and maintainability by rotating
MS/PhD students.

### 1.1 Storage reality (corrected 2026-07-13)

An earlier version of this document claimed *"under 100 GB over the project's life."* **That number was
wrong**, and it was load-bearing rhetoric — so it is corrected here rather than quietly dropped.

| Store | 3-year estimate | Note |
|---|---|---|
| Bronze — institutional PDFs (IDSR, NDMA, sitreps) + cached parses (ADR-012) | ~10–20 GB | ~2,000 documents, few MB each |
| Bronze — news | ~20–60 GB | **Only the relevance-filtered subset** arrives, via the NAaaS API (ADR-013). The unfiltered firehose would have been ~100 GB/yr of raw HTML — this is precisely what the API's keyword gate prevents. |
| Postgres (silver + gold + pgvector) | ~50–150 GB | Panel is a few million rows; pgvector holds only relevant-document chunks |
| Neo4j (CHKG) | ~20–50 GB | ~2–3M nodes |
| Kafka (90-day retention), MLflow artifacts, models, backups, observability | ~200–500 GB | Backups dominate |
| **Total** | **~0.5–1 TB, worst case a few TB** | Comfortably inside 07's 2×4 TB NVMe + 2×8 TB HDD |

**The conclusion survives intact** — a few TB on one box is not a big-data problem — **but the number
must be right**, because the moment someone catches a false premise they will challenge the
conclusion that rests on it. The single biggest lever on this table is the **relevance gate**
(ADR-013): without it, GPU cost, pgvector volume, and CHKG node count all inflate 20–50×.

## 2. Macro architecture

```
┌────────────────────────────── SOURCES ──────────────────────────────┐
│  NIH IDSR (weekly PDF)   Open-Meteo/ERA5   NDMA/PDMA sitreps        │
│                          (PMD pending MOU)                  NAaaS   │
│                                                     (news API: kw + │
│                                                      date, multiling)│
└──────────────┬──────────────────────────────────────────┬───────────┘
               │  Python connectors, ALL Dagster-scheduled, ALL pull
               │  (nothing pushes to us; every source is a poll)
               ▼                                           ▼
    ┌──────────────────────────────────────────────────────────────┐
    │ CONNECTOR (one process per source):                          │
    │   discover → fetch → ARCHIVE-FIRST → parse → validate → produce
    │                          │                        │           │
    │                          ▼                        ▼           │
    └──────────────────────────┼────────────────────────┼───────────┘
                               │ raw bytes              │ parsed records
                               │ + cached parse         │ (CloudEvents)
                               ▼                        ▼
              ┌─────────────────────────┐  ┌──────────────────────────────┐
              │  MinIO (BRONZE)         │  │      KAFKA BACKBONE          │
              │  immutable, permanent   │  │  per-source topics · registry│
              │  SYSTEM OF RECORD       │  │  replayable · dead-letter    │
              │  (claim-check target)   │  │  TRANSPORT, not the archive  │
              └─────────────────────────┘  └──┬──────────────────┬────────┘
                       ▲                      │                  │
                       │ replay_from_bronze   │ Dagster BATCH    │ Spark
                       │ (re-parse, new       │ DRAINS           │ (news NLP
                       │  transform_version)  │ (IDSR/PMD/NDMA)  │  + backfills)
                       │                      ▼                  ▼
                       │             normalizers            NLP enrichment
                       │        (geo → P-code, time →   (lang-ID → NER/RE →
                       │             epi-week)            temporal → geo-link
                       │                      │            → media signals)
                       │                      ▼                  │
┌──────────────────────┴─────  POSTGRESQL (analytical core)  ────┴─────────────┐
│ PostGIS (boundaries, SCD-2) · Timescale (series) · pgvector (RAG embeddings) │
│ silver: normalized per-source  ·  gold: district × epi-week panel            │
└──────┬──────────────────────────────────────┬────────────────────────────────┘
       │                                      │
       ▼                                      ▼
 analytics & forecasting                KG builder (Dagster)
 (GLM/DLNM baselines → Prophet →              ▼
  LSTM; surveillance anomaly            Neo4j — CHKG
  detection; alert candidates)          (evidence-reified edges)
       │                                      │
       │                                graph-RAG (self-hosted LLM,
       │                                 cited, auditable summaries)
       ▼                                      │
┌──────────────────  SERVING CORE (modular monolith)  ─────────────────┐
│ FastAPI: auth · indicators · forecasts · alerts · KG-explorer ·      │
│ RAG summaries · policy-brief exports · admin · audit logging         │
└──────────────────────────────┬────────────────────────────────────────┘
                               ▼
              Dashboard SPA (district choropleths, epi curves vs
              climate overlays, alert center w/ evidence, Urdu/English)
```

### 2.1 Read the ordering carefully: bronze is written *before* Kafka, and Kafka never reads bronze

The previous version of this diagram drew an arrow from MinIO **into** the Kafka backbone, which
implied Kafka consumes from bronze. **It does not.** One connector process writes to both, in this
order (02 §1.2):

> `discover → fetch → **archive_raw (MinIO)** → parse → validate → **produce (Kafka)**`

**Raw bytes** go to MinIO. **Parsed, validated, source-native records** go to Kafka. This is
deliberate, and it is the inverse of the textbook Kafka-first/Kappa pattern. It is right here for
four reasons:

1. **The payloads are blobs, not events.** IDSR bulletins and NDMA sitreps are 1–5 MB PDFs, against
   Kafka's 1 MB default `max.message.bytes`. Any Kafka-first design would be forced into the
   **claim-check pattern** — blob to object storage, pointer on the bus — **which is exactly what
   this is.** CHIP does claim-check correctly; it did not stumble into it.
2. **Retention asymmetry.** Bronze is permanent and is the **system of record**. Kafka retention is
   finite (~90 days) and is **transport**. Kafka is a poor archive: no random access by key without
   scanning a partition, no browsing, no HDD tiering.
3. **Archive-before-parse is a durability guarantee, and parsing *will* fail.** The entire
   quarantine flow (02 §5) exists because NIH will change its bulletin layout. When parse fails, the
   bytes must already be safe.
4. **Two replay tiers, for two different failure modes** (02 §4): re-parse from bronze with a bumped
   `transform_version` (works forever, on any historical document — this is how a better parser fixes
   2022 data), *and* re-consume from Kafka with a new consumer group (fast, but only within
   retention).

## 3. Decision record (ADRs)

| ADR | Decision | Status |
|-----|----------|--------|
| [001](adr/001-service-topology.md) | Event-driven pipeline + modular serving core (no fine-grained microservices, no single monolith) | **Accepted** |
| [002](adr/002-kafka-spark-scope.md) | Kafka carries all sources; Spark scoped to news NLP + backfills; low-frequency consumers are Dagster **batch drains** | **Accepted** (amended 2026-07-13: honest justification; batch-drain rule) |
| [003](adr/003-storage-strategy.md) | PostgreSQL (PostGIS+Timescale+pgvector) + MinIO + Neo4j; nothing else | **Accepted** |
| [004](adr/004-self-hosted-infrastructure.md) | Self-hosted on university hardware, containerized, cloud-agnostic by construction | **Accepted** (institutional constraint) |
| [005](adr/005-canonical-grain-and-provenance.md) | District P-code × epi-week grain; mandatory provenance on every derived record | **Accepted** |
| [006](adr/006-canonical-spatial-key.md) | One canonical spatial key = COD-AB district P-code (GADM/PBS are alias/crosscheck only) | **Accepted** (reconciliation) |
| [007](adr/007-kafka-wire-contract.md) | One Kafka wire contract: CloudEvents envelope + JSON Schema + `chip.<domain>.<source>.<entity>.v<major>` topics | **Accepted** (reconciliation) |
| [008](adr/008-epiweek-convention.md) | Epi-week = WHO/ISO Monday-start, parameterized, validation-gated (OQ-1) | **Accepted** (reconciliation) |
| [009](adr/009-schema-registry.md) | Schema registry = Apicurio (Postgres-backed), not Karapace | **Accepted** (reconciliation) |
| [010](adr/010-kg-consumption-contract.md) | CHKG read contract = reified `:Assertion` nodes; no raw semantic edges | **Accepted** (reconciliation) |
| [011](adr/011-embedding-model.md) | Embedding model = BGE-M3, 1024-dim, for both NLP and RAG | **Accepted** (reconciliation) |
| [012](adr/012-document-parsing.md) | PDF parsing = **agentic parse (LlamaParse)**, output **cached immutably to bronze**; `access_tier` gates cloud parse; in-house parser is an eval track | **Accepted** (2026-07-13) |
| [013](adr/013-news-via-naaas.md) | News arrives via the lab's **NAaaS query API**, not CHIP scrapers; relevance gate for free; requires a count endpoint + doc-durability contract | **Accepted** (2026-07-13) |
| [014](adr/014-gpu-allocation.md) | **2 × 24 GB GPUs**, split GPU0 = resident serving / GPU1 = batch & training | **Accepted** (2026-07-13) |
| [015](adr/015-boundary-versioning-and-revisions.md) | Boundary versioning = 01 §1.4 (SCD-2 + `location_lineage`); IDSR revisions **measured** via `ingestion_revision`, not assumed | **Accepted** (2026-07-13) |

ADRs 006–011 were produced by a cross-subsystem reconciliation pass (2026-07-10): the seven subsystem
docs were written semi-independently and disagreed on shared contracts (spatial key, Kafka envelope/topic
names, epi-week convention, schema registry, KG edge shape, embedding dimension). Each ADR pins the seam to
one decision and supersedes the conflicting statements in the subsystem docs it names.

**ADRs 012–015 came from a second review pass (2026-07-13)** that tested the architecture against four
facts learned after the first draft: the institutional PDFs **require agentic parsing** (the
`pdfplumber`/`camelot` primary path does not work on them); **news comes from the lab's own NAaaS
platform**, not from CHIP scrapers; the GPU budget was sized three different ways in three documents;
and district-boundary versioning was being re-asked as an open question in three subsystems **despite
subsystem 01 having already solved it.** See [`CRITIQUE-AND-OPEN-ISSUES.md`](CRITIQUE-AND-OPEN-ISSUES.md)
for the full analysis behind them.

New significant decisions get a new numbered ADR — never edit history, supersede instead.

## 4. Subsystem micro-plans

| Doc | Subsystem | Owns |
|-----|-----------|------|
| [01](subsystems/01-data-model-and-schemas.md) | Data model & schemas | Gazetteer, epi-week lib, Kafka envelopes/registry, zone model, star schema, drift strategy, data quality |
| [02](subsystems/02-ingestion-connectors.md) | Ingestion & connectors | Connector SDK, per-source specs (IDSR PDFs, PMD, NDMA, news), Dagster scheduling, replay |
| [03](subsystems/03-nlp-pipeline.md) | NLP pipeline | Urdu/English model strategy, NER/RE, annotation program, HeidelTime, geo entity linking, Spark job design, model lifecycle |
| [04](subsystems/04-knowledge-graph-rag.md) | Knowledge graph & RAG | CHKG ontology, evidence reification, Neo4j ops, GNN export, graph-RAG with cited summaries |
| [05](subsystems/05-analytics-forecasting-alerting.md) | Analytics, forecasting & alerting | GLM/DLNM → Prophet → LSTM roadmap, evaluation protocol, anomaly detection, alert lifecycle |
| [06](subsystems/06-serving-dashboard.md) | Serving & dashboard | FastAPI modular monolith, API design, dashboard screens, auth/audit, policy briefs |
| [07](subsystems/07-infrastructure-operations.md) | Infrastructure & operations | Hardware sizing, Compose→k3s, networking/TLS, backups, observability, CI/CD, turnover-proofing |

## 5. Monorepo layout

```
chip/
├── libs/            # shared: schemas, gazetteer, epiweek, provenance, kafka-client, parse-cache
├── connectors/      # one package per source (nih_idsr, pmd_weather, ndma_sitrep, naaas, ...)
├── pipelines/       # normalizers, nlp-enrichment (Spark), kg-builder, panel-materializer
├── services/        # serving core (FastAPI modular monolith), inference service
├── dashboard/       # SPA frontend
├── ml/              # training code, experiments, evaluation harnesses, model cards
├── ontology/        # curated mechanistic priors (cited climate→disease mechanisms) — see §7 Phase 1
├── infra/           # compose stacks, ansible, monitoring, backup configs
└── docs/            # ADRs (mirrored/moved here once repo exists), runbooks, onboarding
```

One repo. Shared libraries are imported, not copied. CI is path-filtered per top-level dir.

## 6. Expandability axes (how CHIP grows without redesign)

1. **New data source** → new connector package + new Kafka topic + registry schema. Nothing else changes.
2. **New disease** → row in the controlled vocabulary (`dim_disease`), not code.
3. **New geography/resolution** (tehsil-level, other countries) → gazetteer version bump; grain is configuration, not assumption.
4. **New model** → registered in MLflow, scored into the same forecast tables with `model_version`; dashboard picks it up.
5. **New language** → language-ID stage routes to a new NLP branch; envelope and storage are language-agnostic.
6. **Better extraction later** → bronze + Kafka replay re-derives everything downstream with a bumped `transform_version`.
7. **New source that *pushes*** → the connector SDK is entirely pull-shaped (`discover → fetch → …`). A pushing source needs a sibling lifecycle (`receive → archive → validate → produce`) behind an authenticated FastAPI ingest endpoint. **Not built; see §6.1.**

### 6.1 On the horizon (deliberately NOT designed yet): a live provincial hospital feed

The stated ambition is that **Punjab's district (DHQ) and tehsil (THQ) hospitals — roughly 200
facilities — report daily, live, into CHIP.** This is parked, not forgotten. What is already known:

- **It is not big data.** Aggregate daily counts ≈ 8k rows/day (~3M/yr, well under 1 GB/yr). Even
  patient-level line lists would be ~100k events/day — **~1–20 events/second**, against a Kafka
  cluster provisioned for millions. The existing architecture absorbs it without a structural change.
- **It is the first source that genuinely justifies Kafka** — many independent producers, small
  event-shaped messages (no claim-check needed), continuous, and the first source that could
  realistically *push* rather than be polled. ADR-002's bet pays off here.
- **The scientific upside is the largest available to this project:** a daily grain moves CHIP from a
  weekly retrospective system to a near-real-time surveillance platform, gives lag models daily
  resolution, and supplies timestamped first-reports — which **solves the vintage problem (ADR-015)
  outright.**
- **The two things that decide it are not technical:**
  1. **One provincial HMIS/DHIS2 endpoint, or 200 facility systems?** If one, it is among the easiest
     connectors in the project. If 200, this is an HMIS integration programme and CHIP should not
     attempt it.
  2. **Aggregate counts, or patient-level records?** Patient-level data would breach the ethics
     envelope the proposal was approved under (*"aggregated at district or higher administrative
     levels… no personally identifiable information"*) and ADR-005's re-identification rationale. It
     would require amended ethics approval, a DSA, de-identification at ingestion, a segregated
     access-controlled zone, and a hard prohibition on third-party APIs touching it (**including the
     ADR-012 cloud parser**).

**Do not design for this until both questions are answered.** Recorded here so the answer isn't
re-derived from scratch when they are.

## 7. Phase plan (macro)

| Phase | Theme | Exit criterion (demoable) |
|-------|-------|---------------------------|
| **0** | Foundations | Monorepo + CI, **gazetteer (SCD-2 + lineage, ADR-015)** & epi-week libs, Kafka+registry+Postgres+MinIO up via Compose, ADR practice running |
| **1** | **Backfill-first vertical slice** | **The historical backfill runs end-to-end** (ADR-012 agentic parse → bronze + cached parse → Kafka → panel), `ingestion_revision` instrumented, weather + NAaaS news joined onto it; dengue + Punjab visible on a minimal dashboard map. **Curated mechanistic prior ontology started.** |
| **2** | NLP | Annotated corpus, fine-tuned NER/RE in production over the NAaaS feed, media-signal facts populating the gold panel. **Mechanistic prior ontology loaded (~50–200 cited assertions).** |
| **3** | Knowledge graph | **10-question Cypher-vs-SQL gate passed** (04 §0.1) → CHKG populated with evidence-reified edges; "why" queries traverse the priors; first graph-RAG cited summaries |
| **4** | Models & alerts | GLM/DLNM baselines beaten-or-not by Prophet/LSTM (honestly reported), anomaly detection live, human-in-the-loop alert workflow + triage |
| **5** | Institutionalization | NIH/PMD/NDMA feeds via MOUs, drift handling proven, hardening, docs/runbooks, stakeholder training |

Each phase ends with something demoable to stakeholders — deliberate alignment with NRPU review cadence.
Phases 2–4 overlap in practice (different students own different tracks); Phase 1 does not start
until Phase 0's gazetteer/epi-week/schema foundations exist.

> **Why Phase 1 is backfill-first, not live-first.** The live pipeline is *trivially small* — a few
> hundred messages/day across three of four sources. Building it first validates nothing. The
> historical backfill, by contrast, is simultaneously (a) the **only training data** the models will
> ever have, (b) the **vintage record** on which every forecast-skill claim depends (ADR-015), and
> (c) the **best possible integration test** of every downstream stage, because it drives a large,
> varied, layout-drifting corpus through parse → normalize → enrich → panel → KG. **If the backfill
> works, live works.** The reverse is not true.

## 8. Proposal-commitment traceability

| Proposal claim | Where it lands |
|----------------|----------------|
| Kafka cluster + producers/topic schemas for NIH, PMD, NDMA/PDMA, news | ADR-002, subsystem 01/02 |
| Spark for NLP workflows over streaming + historical data | ADR-002, subsystem 03 |
| HeidelTime temporal normalization | subsystem 03 |
| District-level geocoding | ADR-005, subsystems 01/03 |
| Fine-tuned transformer NER/RE (BERT/RoBERTa family) | subsystem 03 |
| Climate–Health Knowledge Graph + GNN + graph-RAG | subsystem 04 |
| LSTM + Prophet forecasting, GLM lag/relative-risk models | subsystem 05 |
| Unified schema-controlled repository | ADR-003/005, subsystem 01 |
| Web decision-support dashboard + alerts + explainable summaries | subsystem 06 |
| System documentation, deployment package, manuals | subsystem 07 + docs/ |
| *"Extending the validated NAaaS backbone"* | **ADR-013** — news is consumed from NAaaS rather than re-scraped; this is the *literal* reading of the proposal, not a deviation |

**A note on the two deliverables whose engineering case is weaker than their contractual case.** The
proposal names **Kafka** and **Spark** explicitly, and both are delivered in full and demonstrably.
But neither is justified by CHIP's *throughput* (see ADR-002 §Amendment): the non-news sources total
~200–1,500 messages/day, and there is no genuinely continuous stream anywhere in the platform. They
are kept because they are contracted, because Kafka's fan-out and one-pattern uniformity genuinely
serve the dominant risk (turnover and entropy), and because Spark's unified batch/stream code path is
what runs the historical enrichment. **Everything else in this architecture is sized for the problem
that actually exists** — which is integration and provenance, not volume. Say this plainly in reviews
rather than letting anyone infer a scale that isn't there.
