# Subsystem 04 — Climate–Health Knowledge Graph (CHKG) & Graph-RAG

**Status:** Design (v1) · **Owner:** KG architecture · **Audience:** MS/PhD students building and operating the graph
**Grain (locked, ADR-005):** district × epi-week · **Engines (locked, ADR-003):** Neo4j (graph) · PostgreSQL+pgvector (analytical core) · MinIO (immutable raw docs)
**Deploy (locked, ADR-004):** self-hosted, containerized, 1–2 workstation GPUs, no managed cloud, Neo4j Community Edition (CE)

> **North-star principle:** *The graph is a derived, rebuildable projection.* PostgreSQL (silver/gold panel) and MinIO (raw documents) are the source of truth. Neo4j holds a materialized, query-optimized, provenance-preserving view. Every substantive assertion in the graph must be traceable back to a Postgres row and/or a MinIO document span. This single principle drives the ontology (evidence reification), the pipeline (idempotent MERGE, full rebuild), and operations (backup is a convenience, not a DR necessity).

**Revised 2026-07-13.** ADRs added: **ADR-011** (embeddings = BGE-M3, 1024-dim), **ADR-014** (GPU split — supersedes §6.2's "on 48 GB host Qwen3-32B"), **ADR-015** (boundary versioning — **closes OQ-1**). Two new sections: **§0.1** (the gate that decides whether this subsystem is built at all) and **§0.2** (the missing workstream without which it cannot answer "why").

---

## 0.1 ⚠️ GATE: prove the graph earns Neo4j before Phase 3 builds it

**This subsystem's *design* is sound. Its *justification* is the thinnest in the architecture, and it must be tested rather than assumed.** The question that decides it:

> **What can the CHKG answer that a SQL join on the gold panel cannot?**

Run the audit honestly and most stakeholder questions come back **SQL-shaped**:

| A question a real NIH/NDMA analyst asks | Needs a graph? |
|---|---|
| "Dengue cases in Lahore, W27, with the preceding 3 weeks of rainfall" | **No.** SQL, on the panel. |
| "Why was dengue flagged in Lahore?" — feature attributions, climate anomaly, model version, data vintage | **No.** That is 05 §5.7's evidence bundle: a JSONB column. |
| "Which news articles support the flood→cholera claim in Larkana?" | **No.** A join to `documents` + pgvector. |
| "Traverse: HeavyRainfall → StandingWater → AedesBreeding → Dengue, with evidence at each hop" | **YES.** Variable-length multi-hop over a mechanistic chain. Recursive CTEs *can*; Cypher does it far better. |
| "Find claims structurally similar to this one, in districts we have not yet flagged" | **YES.** Genuinely graph-shaped. |

**Neo4j's unique, irreplaceable value is exactly two things:** (1) **multi-hop explainability traversal** over curated mechanistic priors + observed facts + extracted claims, and (2) **the graph-RAG retrieval skeleton** (§6.1) — the subgraph supplies the *reasoning structure with epistemic status*, pgvector supplies the *prose evidence*.

It is **not** needed for analytics (SQL) and it is **not** needed for the GNN — §5.3 exports Neo4j → pandas → PyG, and one could go **Postgres → pandas → PyG** and skip the graph entirely for the ML path.

That is fine: (1) and (2) *are* the funded deliverables — *"explainable analytics… evidence-backed and auditable summaries for policy stakeholders."* But **be clear-eyed:** if the CHKG ends up a thin wrapper over the panel plus a handful of priors, the "why" queries will be shallow and the RAG summaries no better than templating the alert's evidence bundle.

### The gate (one week, one student, zero infrastructure)

**Before Phase 3 builds the CHKG:** write down **10 questions a real NIH/NDMA analyst would ask**. For each, write **both the Cypher and the SQL**.

- **≥ 7 strictly easier or only possible in Cypher** → the graph earns Neo4j. Build as designed.
- **Most are SQL-shaped** → **cut Neo4j's scope to the RAG evidence layer only** (`Document`, `Evidence`, `Assertion`) and keep the facts in Postgres, where they already live.

This converts the largest unjustified component in the architecture into a decision backed by evidence, for the price of one student-week. **Do not skip it.**

---

## 0.2 ⚠️ The missing workstream: the mechanistic prior ontology

**Without this, §0.1's gate fails automatically — because the multi-hop traversal that justifies the graph has nothing to traverse.**

05 §5.7 promises that every issued alert carries a KG path as its explanation, and gives this example:

```
HeavyRainfall(district, wk t-3) → StandingWater → AedesBreeding → Dengue(district)
```

**Ask where `StandingWater` and `AedesBreeding` come from.** Not NIH (they report case counts). Not PMD (millimetres of rain). Not NDMA (flood extents). Not the news (an article says *"dengue rose after the floods"*; it does not say *"Aedes oviposition sites increased"*).

**They are disease biology.** They exist only in the epidemiological literature, and somebody must author them into the graph by hand. §1.2 anticipates this — it defines `derivation: 'domain_prior'`, and §4.4's governance query even *exempts* such assertions from the evidence requirement — **but no subsystem document creates them. There is no owner, no workstream, and no deliverable.**

### Decision: a curated, cited prior ontology is a Phase-1/2 deliverable

- **Scope:** ~50–200 assertions. Each is an `:Assertion` node with `assertion_type ∈ {TRIGGERS, ASSOCIATED_WITH}`, `status: 'hypothesized'` (or `'established'` where the literature is settled), and `derivation: 'domain_prior'`.
- **Every prior cites a peer-reviewed paper as its `:Evidence`** (DOI → an `:Evidence` node → a `:Document` for the paper). **This removes the `domain_prior` exemption from §4.4's audit query entirely** — *every* claim in the CHKG then carries evidence, with no exceptions. That is a strictly better invariant and it should be adopted.
- **Content:** the mechanistic chains for the v1 disease set. E.g. *rainfall → standing water → Aedes breeding → dengue (with the extrinsic incubation period as a parameter)*; *flooding → drinking-water contamination → cholera (incubation hours–5 days)*; *temperature/smog → ARI*. These are precisely the lag windows 05 §2.2 already hard-codes — **the ontology is the *citable justification* for the numbers the models already use.**
- **Storage:** `ontology/priors.yaml` in the monorepo, PR-reviewed, loaded by a Dagster asset. Versioned like code.
- **Owner:** an MS student doing a structured literature review, **validated by the epidemiology domain partner.** The proposal names *"limited in-house domain expertise in epidemiology"* as a launch weakness — **this deliverable lands squarely on that gap, and is the concrete answer to it.**

**Why it is worth the effort:** it is what turns *"cases are up and it rained a lot"* (a correlation restated) into *"heavy rainfall three weeks ago created breeding conditions; Aedes development takes ~2 weeks; this district's dengue is now rising, consistent with the mechanism [cite]"* — **an explanation.** That difference is the entire product.

---

## 0. Design decisions at a glance

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | **Reify observations and claims as nodes**, not time-stamped edges | Neo4j relationships cannot themselves carry relationships; evidence + confidence + epistemic status must attach as sub-structure. Reification is the only way to satisfy ADR-005 provenance. Also cleaner for GNN export. |
| D2 | **`EpiWeek` is a first-class node** on a `PRECEDES` chain (temporal backbone) | Canonical grain is epi-week; a materialized time spine makes lag queries, windowing, and temporal-GNN slicing trivial in Cypher. |
| D3 | **Two provenance tiers:** structured facts → `PROVENANCE` to Postgres row keys; text-extracted claims → `SUPPORTED_BY` to `:Evidence` (MinIO URI + offsets + confidence + model version) | Panel facts are already auditable in Postgres; NLP-derived claims need document-span traceability. |
| D4 | **Epistemic status is explicit** on every claim (`observed` / `statistical` / `hypothesized` / `refuted`) | `TRIGGERS` is a hypothesis, not a fact. Auditability = never presenting a hypothesized causal link as ground truth. |
| D5 | **Neo4j CE only**; graph treated as rebuildable → cold `dump` backups suffice | ADR-004 budget; source-of-truth lives elsewhere so RPO tolerance is high. |
| D6 | **Graph ML is research-track first**; link-prediction + temporal risk propagation are the two funded first tasks; predictions flow back as `status:'statistical'` claims with provenance | Honest scoping: at tens-of-thousands of nodes, classical stats (GLM/DLNM) is the production baseline; GNNs earn their place via theses, and any GNN output is a *cited, dated, model-versioned* claim — never an unattributed edge. |
| D7 | **Graph-RAG = subgraph retrieval (Neo4j) ⊕ semantic chunk retrieval (pgvector)**, answer-only-from-context, mandatory inline citations, post-hoc citation verification | Contractual explainability/auditability. |
| D8 | **Self-hosted LLM:** Qwen3-family (14B/32B) via **vLLM** as the workhorse; Gemma 3 (12B/27B, 140+ languages) as multilingual fallback; Urdu-heavy generation via a Qalb-style Urdu model | Fits a 24–48 GB workstation GPU; strong multilingual + Urdu (see §6.2, verified July 2026). |

---

## 1. CHKG Ontology

### 1.1 Node labels

Two conceptual layers: **canonical/dimension nodes** (the vocabulary — stable, deduplicated) and **assertion/observation nodes** (the reified facts and claims — high volume, evidence-bearing).

```
                          CANONICAL (dimension) LAYER
   ┌──────────┐  ┌──────────┐  ┌────────────────┐  ┌────────────┐  ┌──────────────┐
   │ Disease  │  │ Symptom  │  │ ClimateVariable│  │ Location   │  │ Organization │
   │ (ICD-10) │  │          │  │  (temp,precip) │  │ (district) │  │ (NIH,NDMA…)  │
   └──────────┘  └──────────┘  └────────────────┘  └────────────┘  └──────────────┘
        ▲             ▲               ▲                   ▲                ▲
        │             │               │                   │                │
   ─────┼─────────────┼───────────────┼───────────────────┼────────────────┼────────
        │             │        REIFIED FACT / CLAIM LAYER  │                │
        │      ┌──────────────┐  ┌──────────────────┐  ┌──────────────┐     │
        └──────┤ DiseaseObs   │  │ ClimateObs       │  │ HazardEvent  ├─────┘
   OF_DISEASE  │ (cases/week) │  │ (value/week)     │  │ (flood,heat) │
               └──────┬───────┘  └────────┬─────────┘  └──────┬───────┘
                      │  DURING           │ DURING            │ DURING / OCCURRED_IN
                      ▼                    ▼                   ▼
                 ┌─────────────────────────────────────────────────┐
   PRECEDES ───► │  EpiWeek  ──PRECEDES──►  EpiWeek  ──PRECEDES──►  │   (time spine)
                 └─────────────────────────────────────────────────┘
                      ▲
                      │ DURING
               ┌──────────────┐        ┌───────────────────────────────┐
               │ MediaSignal  │        │        Assertion              │   (n-ary claim:
               │ (news mention)│       │  status∈{observed,statistical,│    ASSOCIATED_WITH,
               └──────┬───────┘        │  hypothesized,refuted}        │    TRIGGERS, PRECEDES)
                      │ SUPPORTED_BY   └───┬─────────┬──────────┬──────┘
                      ▼                    │SUBJECT  │OBJECT    │SUPPORTED_BY
               ┌──────────────┐            ▼         ▼          ▼
               │  Evidence    │◄───────(canonical nodes)   ┌──────────────┐
               │ (doc span)   │────DERIVED_FROM───────────►│  Document    │
               └──────────────┘                           │ (MinIO URI)  │
                                                           └──────────────┘
   ┌──────────┐  ┌──────────┐   Optional / secondary:
   │ AlertRisk│  │ RiskModel│   AlertRisk = a flagged risk for (Disease,Location,EpiWeek);
   │ (flag)   │  │ (run)    │   RiskModel = a model version + run that produced statistical claims.
   └──────────┘  └──────────┘
```

**Label catalogue**

| Label | Kind | Meaning | Natural key |
|-------|------|---------|-------------|
| `Disease` | canonical | Climate-sensitive disease (dengue, malaria, cholera, ARI…) | `icd10` (or curated `disease_code`) |
| `Symptom` | canonical | Clinical sign extracted from text | `symptom_id` (UMLS/curated) |
| `ClimateVariable` | canonical | Meteorological indicator (temp_mean, precip_total, humidity, AQI…) | `variable_code` |
| `Location` | canonical | Administrative unit; **district is the grain**, with `PARENT_OF` up to province/country | `pcode` (COD-AB admin2 P-code — ADR-006) |
| `EpiWeek` | canonical/spine | Epidemiological week (MMWR/ISO) | `epi_week_id` = `YYYY-Www` |
| `Organization` | canonical | Data custodian / stakeholder (NIH, NDMA, PMD, PDMA…) | `org_id` |
| `Document` | evidence | One source document (news article, IDSR bulletin, sitrep) | `doc_id` (hash of MinIO object) |
| `Evidence` | evidence | A specific text span within a Document supporting one claim | `evidence_id` (uuid) |
| `DiseaseObs` | fact | Reified panel fact: cases of Disease in Location during EpiWeek | `(disease_code, pcode, epi_week_id)` |
| `ClimateObs` | fact | Reified panel fact: value of ClimateVariable in Location during EpiWeek | `(variable_code, pcode, epi_week_id)` |
| `HazardEvent` | fact | Discrete disaster event (flood, heatwave, drought) | `hazard_event_id` |
| `MediaSignal` | fact | Aggregated media mention signal (disease/hazard buzz) per Location×EpiWeek | `(signal_type, pcode, epi_week_id)` |
| `Assertion` | claim | N-ary reified claim linking canonical nodes with epistemic status | `assertion_id` (uuid, deterministic — see §2.4) |
| `AlertRisk` | derived | A risk flag raised for (Disease, Location, EpiWeek) | `(disease_code, pcode, epi_week_id, model_run_id)` |
| `RiskModel` | metadata | A model + version + training run | `model_run_id` |

### 1.2 Relationship types & semantics

| Type | From → To | Semantics | Epistemic? | Key properties |
|------|-----------|-----------|------------|----------------|
| `OF_DISEASE` | DiseaseObs → Disease | structural link | definitional | — |
| `OF_VARIABLE` | ClimateObs → ClimateVariable | structural | definitional | — |
| `IN_LOCATION` | *Obs/HazardEvent/MediaSignal* → Location | spatial anchor | definitional | — |
| `DURING` | *Obs/HazardEvent/MediaSignal/Assertion* → EpiWeek | temporal anchor | definitional | — |
| `OCCURRED_IN` | HazardEvent → Location | event location (may span multiple districts) | definitional | `coverage` (full/partial) |
| `PRECEDES` | EpiWeek → EpiWeek | time-spine ordering (Δ = 1 week) | definitional | `weeks_gap` |
| `PARENT_OF` | Location → Location | admin hierarchy (province→district) | definitional | `admin_level` |
| `REPORTED_IN` | *canonical/fact node* → Document | "entity/fact mentioned in this document" | provenance | `mention_count` |
| `DERIVED_FROM` | Evidence → Document | evidence span belongs to document | provenance | `char_start`,`char_end` |
| `SUPPORTED_BY` | Assertion → Evidence | claim justified by this span | provenance | — |
| `PROVENANCE` | fact node → (external ref) | fact traces to Postgres row (stored as property, see §1.4) | provenance | — |
| `SUBJECT` / `OBJECT` | Assertion → canonical node | the two poles of an n-ary claim | — | `role` |
| `PRODUCED` | RiskModel → Assertion/AlertRisk | model run authored this claim | provenance | — |
| `FLAGS` | AlertRisk → DiseaseObs | the observation the alert is about | — | `score` |

**Semantic claim types** are expressed as `Assertion` nodes with `assertion_type` ∈:

| `assertion_type` | Meaning | Typical `status` | Typical `derivation` |
|------------------|---------|------------------|----------------------|
| `ASSOCIATED_WITH` | statistical co-movement between a ClimateVariable and a Disease | `statistical` | `glm` / `dlnm` / `link_prediction` |
| `CO_OCCURS_WITH` | temporal/spatial co-occurrence (descriptive) | `observed` | `panel_aggregation` |
| `TRIGGERS` | **hypothesized** causal mechanism (e.g. flood → cholera) | `hypothesized` | `nlp_causal` / `domain_prior` |
| `PRECEDES_SIGNAL` | media signal leads official notification | `observed`/`statistical` | `lead_lag` |

> **Why `TRIGGERS` is never a raw edge.** A direct `(:HazardEvent)-[:TRIGGERS]->(:Disease)` edge would silently assert causation. Instead we mint an `Assertion {assertion_type:'TRIGGERS', status:'hypothesized', confidence, derivation, model_version}` with `SUBJECT`/`OBJECT`/`SUPPORTED_BY`. Every consumer (Cypher, GNN, RAG) must read `status` and surface it honestly ("hypothesized association, 2 supporting reports").

### 1.3 Evidence reification — the core provenance mechanism

Neo4j relationships cannot point to other relationships, so any assertion that needs **multiple evidence links + confidence + model version** must be a node. Pattern:

```cypher
// A hypothesized flood→cholera trigger, extracted from two news documents
MERGE (a:Assertion {assertion_id: $aid})
  ON CREATE SET a.assertion_type='TRIGGERS',
                a.status='hypothesized',
                a.confidence=0.62,
                a.derivation='nlp_causal',
                a.model_version='relx-roberta-v3.2',
                a.created_at=datetime(), a.first_batch=$batch
  ON MATCH  SET a.confidence=0.62, a.last_batch=$batch
WITH a
MATCH (h:HazardEvent {hazard_event_id:$hid})
MATCH (d:Disease {icd10:'A00'})            // cholera
MERGE (a)-[:SUBJECT {role:'hazard'}]->(h)
MERGE (a)-[:OBJECT  {role:'disease'}]->(d)
MERGE (a)-[:DURING]->(:EpiWeek {epi_week_id:$week})
WITH a
UNWIND $evidence AS ev
  MERGE (e:Evidence {evidence_id: ev.evidence_id})
    ON CREATE SET e.minio_uri = ev.minio_uri,
                  e.char_start = ev.char_start,
                  e.char_end   = ev.char_end,
                  e.quote      = ev.quote,
                  e.extraction_confidence = ev.conf,
                  e.model_version = ev.model_version
  MERGE (doc:Document {doc_id: ev.doc_id})
    ON CREATE SET doc.minio_uri = ev.doc_minio_uri, doc.source = ev.source,
                  doc.published_at = datetime(ev.published_at)
  MERGE (e)-[:DERIVED_FROM {char_start:ev.char_start, char_end:ev.char_end}]->(doc)
  MERGE (a)-[:SUPPORTED_BY]->(e);
```

**`Evidence` node properties (contract).** Every text-derived claim resolves to at least one `Evidence`:

| Property | Type | Notes |
|----------|------|-------|
| `evidence_id` | string (uuid) | primary key |
| `minio_uri` | string | `s3://chip-raw/<bucket>/<object>` of the exact source object |
| `doc_id` | string | fk to `Document` |
| `char_start`,`char_end` | int | offsets into the **normalized** document text stored alongside raw in MinIO |
| `quote` | string | the literal supporting sentence(s) — used verbatim by RAG for citation |
| `extraction_confidence` | float | model score for the extraction |
| `model_version` | string | NER/RE model id + version (e.g. `relx-roberta-v3.2`) |
| `extracted_at` | datetime | audit timestamp |

Structured panel facts carry **structured provenance** instead (they are already auditable in Postgres) — see §1.4.

### 1.4 Property conventions

- **Every node** carries: `_source_system` (`postgres_gold` / `nlp_pipeline` / `manual`), `_created_at`, `_last_batch` (Dagster run/partition id), `_valid` (bool, soft-delete tombstone).
- **Structured provenance** on fact nodes: `pg_table`, `pg_row_key` (JSON of the natural key), `pg_loaded_at`. This is the panel-fact equivalent of `SUPPORTED_BY` — an auditor can `SELECT` the exact row.
- **Confidence** lives on `Assertion`/`Evidence`, never assumed on definitional edges.
- **Naming:** node labels `PascalCase`, relationship types `SCREAMING_SNAKE`, properties `snake_case`. Internal/bookkeeping properties prefixed `_`.
- **Datetimes** are Neo4j `datetime` (UTC). Epi-week is the modeling clock; wall-clock timestamps are for audit only.

### 1.5 Temporal modeling — decision & rationale

**Choice: reified fact/event nodes anchored to a materialized `EpiWeek` spine, NOT time-stamped edges or per-week edge copies.**

Considered alternatives:

| Option | Sketch | Verdict |
|--------|--------|---------|
| **A. Time-stamped edges** | `(:Disease)-[:CASES {week, count}]->(:Location)` | ✗ can't attach evidence/confidence sub-structure; can't reify a claim about a claim; explodes into parallel edges; awkward GNN export. |
| **B. Time-sliced graph copies** | one subgraph per week | ✗ storage blowup, painful cross-week (lag) queries, duplication of canonical nodes. |
| **C. Reified obs/event nodes + EpiWeek spine** *(chosen)* | `(:DiseaseObs)-[:OF_DISEASE]->(:Disease)`, `-[:IN_LOCATION]->`, `-[:DURING]->(:EpiWeek)` | ✓ evidence/confidence attach to the node; ✓ lag queries walk the `PRECEDES` spine; ✓ one row → one node maps cleanly to Postgres; ✓ direct heterogeneous-graph export for PyG. |

The `EpiWeek` spine (`(:EpiWeek)-[:PRECEDES]->(:EpiWeek)`) makes lagged-exposure queries — the epidemiological heart of this project — a fixed-length path walk:

```cypher
// climate exposure 2 epi-weeks before a disease observation, same district
MATCH (do:DiseaseObs)-[:DURING]->(w:EpiWeek)
MATCH (w2:EpiWeek)-[:PRECEDES*2]->(w)
MATCH (co:ClimateObs)-[:DURING]->(w2)
WHERE (do)-[:IN_LOCATION]->(:Location)<-[:IN_LOCATION]-(co)
RETURN do, co;
```

This design serves both Cypher (walk the spine) and GNN export (§5.3: `EpiWeek` becomes a temporal index; obs nodes become typed feature-bearing nodes).

---

## 2. Population pipeline (Dagster, idempotent)

### 2.1 Sources → graph mapping

| Graph node/claim | Source | Cadence |
|------------------|--------|---------|
| Canonical dims (Disease, ClimateVariable, Location, Organization, EpiWeek) | Postgres **dimension** tables + curated vocabularies | rarely (on vocab change) |
| `DiseaseObs`, `ClimateObs`, `HazardEvent`, `MediaSignal` | Postgres **gold** panel (`district × epi-week`) | weekly, per epi-week partition |
| `Document`, `Evidence`, `Assertion`, `REPORTED_IN` | NLP pipeline **enriched-document** outputs (entities, relations, offsets, geocode, normalized time) | as documents land |
| `AlertRisk`, `RiskModel`, statistical `Assertion`s | analytics/ML jobs writing back | per model run |

### 2.2 Dagster asset graph

```
  dim_vocab_tables ─┐
                    ├──► chkg_canonical_nodes ─┐
  pg_dim_tables ────┘                          │
                                               ├──► chkg_facts (partitioned by epi_week)
  pg_gold_panel (epi_week partition) ──────────┘        │
                                                        ├──► chkg_documents_evidence
  nlp_enriched_docs (partitioned by ingest_date) ───────┘        │
                                                                 ├──► chkg_assertions
  analytics_glm / ml_link_pred (model_run) ──────────────────────┘        │
                                                                          ▼
                                                                 chkg_indexes_checks
                                                                 (constraint + provenance QA)
```

- **Partitioning:** `chkg_facts` uses Dagster **TimeWindowPartitions keyed by epi-week**; `chkg_documents_evidence` uses **daily ingest partitions**. A weekly schedule materializes the newest epi-week partition; a sensor materializes document partitions as MinIO/Postgres emit new enriched docs.
- **Isolation:** each asset opens a Neo4j session via a shared `Neo4jResource`; writes are batched with `UNWIND $rows` (500–2 000 rows/tx) inside explicit transactions.

### 2.3 Idempotent MERGE patterns

**All writes are `MERGE` on natural keys** so re-running a partition is a no-op-or-update, never a duplicate.

```cypher
// Canonical Disease (idempotent upsert)
UNWIND $rows AS r
MERGE (d:Disease {icd10: r.icd10})
  ON CREATE SET d.name=r.name, d.synonyms=r.synonyms,
                d._source_system='postgres_gold', d._created_at=datetime(), d._valid=true
  ON MATCH  SET d.name=r.name, d.synonyms=r.synonyms;

// Reified DiseaseObs on composite natural key
UNWIND $rows AS r
MERGE (do:DiseaseObs {disease_code:r.disease_code, pcode:r.pcode, epi_week_id:r.epi_week_id})
  ON CREATE SET do._created_at=datetime()
  SET do.case_count=r.case_count, do.incidence=r.incidence,
      do.pg_table='gold.epi_panel', do.pg_row_key=r.pg_row_key,
      do.pg_loaded_at=datetime(r.loaded_at), do._last_batch=$batch, do._valid=true
WITH do, r
MATCH (d:Disease {icd10:r.disease_code})
MATCH (l:Location {pcode:r.pcode})
MATCH (w:EpiWeek {epi_week_id:r.epi_week_id})
MERGE (do)-[:OF_DISEASE]->(d)
MERGE (do)-[:IN_LOCATION]->(l)
MERGE (do)-[:DURING]->(w);
```

**Rules:**
1. `MERGE` only on the **natural key**; set all other properties with `SET` (so re-ingest corrects drift).
2. Never `MERGE` on a whole property set (that creates a new node when any property changes).
3. Create the `EpiWeek` spine once, ahead of facts (a bootstrap asset generates all weeks in range with `PRECEDES`).
4. Bump `_last_batch` on every touch; a stale `_last_batch` on a full-refresh partition marks candidates for tombstoning.

### 2.4 Deterministic assertion IDs & entity resolution

- **Deterministic `assertion_id`:** `sha1(assertion_type | subject_key | object_key | epi_week_id | derivation)`. Re-extraction of the same claim from a *new* document adds an extra `SUPPORTED_BY` edge to the *same* `Assertion` (accumulating evidence, raising effective confidence) rather than duplicating it.
- **Entity resolution before the graph, not in it.** The NLP pipeline emits a **surface form + candidate canonical id + match score**. Canonicalization uses:
  - Diseases → curated dictionary (ICD-10 + synonym list, Urdu/English aliases).
  - Locations → PBS/HDX district PCODEs via the geocoder.
  - Time → normalized epi-week from HeidelTime output.
  - Organizations → org registry.
- **Alias table** (`silver.entity_alias`: `surface_form → canonical_id, score, resolved_by`) is maintained in Postgres and is itself auditable. Graph MERGE always uses the **canonical id**.

**Collision handling (do not auto-merge ambiguous entities):**

```
NLP emits entity mention
        │
        ▼
 score ≥ 0.90 (unique best) ──yes──► MERGE on canonical id
        │ no
        ▼
 multiple candidates within Δ0.05, OR best < 0.90
        │
        ▼
 create (:EntityResolutionCandidate {surface, candidates[], scores[], status:'pending'})
 link Evidence to it; DO NOT attach to canonical graph
        │
        ▼
 curator resolves → writes alias → next run MERGEs to chosen canonical id
```

This keeps the canonical graph clean and makes every merge decision reviewable — an ADR-005 requirement.

### 2.5 Incremental updates

- Weekly: materialize the new `EpiWeek` partition → upserts that week's `*Obs`, `HazardEvent`, `MediaSignal`; extends the `PRECEDES` spine.
- Documents: sensor-driven; upserts `Document`/`Evidence`/`Assertion`, accumulating `SUPPORTED_BY`.
- Late-arriving / corrected panel rows: re-materialize the affected epi-week partition; `MERGE`+`SET` overwrites in place (idempotent). Use `pg_loaded_at` to detect corrections.
- **Retractions:** if a source row is deleted/invalidated, set `_valid=false` (tombstone) rather than deleting, preserving audit trail; queries filter `WHERE n._valid`. A monthly compaction hard-deletes tombstones older than the retention window.

### 2.6 Rebuild-from-scratch

Because the graph is a projection, a clean rebuild is always available and is the primary "backup":

1. **Fast path (bulk import, empty DB):** export Postgres gold + dim + assertion tables to CSV; stop Neo4j; `neo4j-admin database import full chkg --nodes=... --relationships=...`; start; run `chkg_indexes_checks`. Loads millions of edges in minutes.
2. **Incremental path (running DB):** trigger a Dagster **backfill** across all epi-week + document partitions. Slower (MERGE per row) but online-safe and reuses the exact production code path.
3. Post-rebuild QA (`chkg_indexes_checks`): assert constraint existence, zero orphan `Evidence`, every `Assertion` has ≥1 `SUPPORTED_BY`, every `DiseaseObs` has `pg_row_key`, node/edge counts within ±ε of Postgres aggregates.

---

## 3. Neo4j operations (Community Edition)

### 3.1 CE constraints and mitigations (verified July 2026)

Neo4j moved Enterprise to an **open-core** model; **Community Edition remains GPLv3, free, ACID, single instance**. What CE lacks and how we cope:

| CE limitation | Impact | Mitigation (within ADR-003/004) |
|---------------|--------|---------------------------------|
| **No clustering / HA** | single point of failure | Acceptable: graph is rebuildable (§2.6); target availability is business-hours analytics, not 24/7 OLTP. |
| **No online/hot backup** (`neo4j-admin database backup` is Enterprise) | can't snapshot a live DB | Scheduled **cold `dump`** in a short nightly maintenance window (§3.4); source-of-truth is Postgres/MinIO so RPO is generous. |
| **No differential backup** | full dump each time | Graph is modest (tens of thousands of nodes, ≤ low-millions edges); a full dump is small and fast. |
| **Single database** (CE serves one user DB) | no per-tenant DBs | Fine: one CHKG. Use labels, not separate DBs. |
| **No fine-grained RBAC** | coarse auth only | Put Neo4j behind the app tier; no direct external Bolt exposure. |
| **GDS concurrency capped (community/OpenGDS runs algorithms at ≤ 4 cores; ≤ 3 stored ML models)** | slower graph algos, few in-DB models | At our scale, 4 cores is ample; heavy GNN training happens in PyG outside Neo4j anyway (§5). |

### 3.2 Constraints & indexes DDL

```cypher
// ---- Node key / uniqueness constraints (also create backing indexes) ----
CREATE CONSTRAINT disease_key   IF NOT EXISTS FOR (d:Disease)        REQUIRE d.icd10 IS UNIQUE;
CREATE CONSTRAINT climvar_key   IF NOT EXISTS FOR (c:ClimateVariable)REQUIRE c.variable_code IS UNIQUE;
CREATE CONSTRAINT location_key  IF NOT EXISTS FOR (l:Location)       REQUIRE l.pcode IS UNIQUE;
CREATE CONSTRAINT epiweek_key   IF NOT EXISTS FOR (w:EpiWeek)        REQUIRE w.epi_week_id IS UNIQUE;
CREATE CONSTRAINT org_key       IF NOT EXISTS FOR (o:Organization)   REQUIRE o.org_id IS UNIQUE;
CREATE CONSTRAINT doc_key       IF NOT EXISTS FOR (d:Document)       REQUIRE d.doc_id IS UNIQUE;
CREATE CONSTRAINT evidence_key  IF NOT EXISTS FOR (e:Evidence)       REQUIRE e.evidence_id IS UNIQUE;
CREATE CONSTRAINT assertion_key IF NOT EXISTS FOR (a:Assertion)      REQUIRE a.assertion_id IS UNIQUE;

// Composite node keys for reified facts (CE supports single-property uniqueness;
// enforce composites via a synthetic key property built from the natural key)
CREATE CONSTRAINT diseaseobs_key IF NOT EXISTS FOR (o:DiseaseObs) REQUIRE o.obs_key IS UNIQUE; // obs_key = disease_code|pcode|epi_week_id
CREATE CONSTRAINT climateobs_key IF NOT EXISTS FOR (o:ClimateObs) REQUIRE o.obs_key IS UNIQUE;
CREATE CONSTRAINT hazard_key     IF NOT EXISTS FOR (h:HazardEvent) REQUIRE h.hazard_event_id IS UNIQUE;

// ---- Secondary indexes for query performance ----
CREATE INDEX obs_week   IF NOT EXISTS FOR (o:DiseaseObs) ON (o.epi_week_id);
CREATE INDEX climobs_wk IF NOT EXISTS FOR (o:ClimateObs) ON (o.epi_week_id);
CREATE INDEX doc_pub    IF NOT EXISTS FOR (d:Document)   ON (d.published_at);
CREATE INDEX assert_typ IF NOT EXISTS FOR (a:Assertion)  ON (a.assertion_type, a.status);
CREATE FULLTEXT INDEX doc_text IF NOT EXISTS FOR (d:Document) ON EACH [d.title, d.summary];
```

> Note: CE offers single-property uniqueness constraints; emulate composite keys with a concatenated `obs_key`/`assertion_id` synthetic property (also what MERGE keys on). Node-key (multi-prop) constraints are an Enterprise feature.

### 3.3 Memory sizing

Graph is small, so size for **page cache holding the whole store** plus modest heap. For a workstation with 64 GB RAM sharing GPU inference duties:

```
# neo4j.conf (illustrative for ≤ low-millions edges)
server.memory.heap.initial_size=4g
server.memory.heap.max_size=8g
server.memory.pagecache.size=8g      # ≥ total store size on disk; graph fits fully in cache
```

Guideline: `pagecache ≥ size of $NEO4J_HOME/data/databases/* on disk` (aim to cache 100%); `heap` 4–8 GB is plenty for our query/transaction sizes. Leave the bulk of RAM/VRAM for the LLM and GNN jobs. Re-check with `CALL dbms.listPools()` / store size after first full load.

### 3.4 Backup / restore procedure

```bash
# NIGHTLY COLD BACKUP (maintenance window; CE requires the DB stopped/offline for dump)
docker compose stop neo4j
docker run --rm -v chkg_data:/data -v /backups:/backups neo4j:community \
  neo4j-admin database dump neo4j --to-path=/backups
docker compose start neo4j
# rotate: keep 7 daily + 4 weekly; push a copy to MinIO (versioned, immutable bucket)
mc cp /backups/neo4j-$(date +%F).dump myminio/chip-backups/neo4j/

# RESTORE
docker compose stop neo4j
docker run --rm -v chkg_data:/data -v /backups:/backups neo4j:community \
  neo4j-admin database load neo4j --from-path=/backups --overwrite-destination=true
docker compose start neo4j
```

- **RPO:** ≤ 24 h from dumps, effectively **0** because a Dagster backfill reconstructs the graph from Postgres/MinIO.
- **RTO:** minutes (load a dump) or ~1 backfill run (rebuild).
- Verify each dump: on a throwaway container, `load` it and run the §2.6 QA checks. A backup that hasn't been test-restored isn't a backup.

### 3.5 When to consider alternatives

Stay on **Neo4j CE**. Escalate only if a *contractual* need appears that CE cannot meet:

| Trigger | Option (still ADR-compatible) |
|---------|-------------------------------|
| Contractual 24/7 uptime / online backup SLA | **Neo4j Enterprise** on-prem via an **academic/research license** (Neo4j offers free/discounted licenses for education/research — apply through the university). Stays self-hosted, honors ADR-003. |
| GDS concurrency becomes a training bottleneck in-DB | Move graph ML fully to **PyG offline** (already the plan, §5) — no engine change. |
| Genuinely huge graph (≫ tens of millions of nodes) | Out of scope for this project's stated scale; revisit only with new requirements. Do **not** introduce a second graph engine (ADR-003 locks engines). |

Managed cloud (AuraDB) and non-Neo4j engines (Memgraph, etc.) are **out** per ADR-003/004.

---

## 4. Explainability queries

Goal: answer *"Why did the system flag dengue risk in district X this epi-week?"* by tracing **alert → contributing signals → evidence documents**, with epistemic status attached.

### 4.1 Full trace: alert → signals → evidence

```cypher
// Q: Why was dengue flagged in <pcode> for <epi_week>?
WITH $pcode AS pcode, $week AS week, 'DENGUE' AS disease_code
MATCH (al:AlertRisk {disease_code:disease_code, pcode:pcode, epi_week_id:week})
MATCH (al)-[f:FLAGS]->(do:DiseaseObs)
OPTIONAL MATCH (al)<-[:PRODUCED]-(m:RiskModel)
// contributing statistical / hypothesized claims about this disease & district window
OPTIONAL MATCH (a:Assertion)-[:OBJECT]->(:Disease {icd10:disease_code})
WHERE (a)-[:DURING]->(:EpiWeek {epi_week_id:week})
OPTIONAL MATCH (a)-[:SUBJECT]->(driver)          // ClimateVariable / HazardEvent
OPTIONAL MATCH (a)-[:SUPPORTED_BY]->(e:Evidence)-[:DERIVED_FROM]->(doc:Document)
RETURN al.score                              AS alert_score,
       m.model_run_id                        AS model,
       do.case_count                         AS observed_cases,
       a.assertion_type                      AS driver_type,
       a.status                              AS epistemic_status,   // observed/statistical/hypothesized
       a.confidence                          AS confidence,
       labels(driver)                        AS driver_label,
       coalesce(driver.name, driver.hazard_event_id) AS driver,
       e.quote                               AS evidence_quote,
       doc.source                            AS source,
       e.minio_uri                           AS source_uri
ORDER BY confidence DESC;
```

Returns rows like: *alert_score 0.81 · model risk-glm-2026w27 · observed_cases 34 · driver_type ASSOCIATED_WITH · status statistical · confidence 0.73 · ClimateVariable "precip_total" ...* alongside *driver_type TRIGGERS · status hypothesized · "heavy monsoon rains flooded low-lying areas of X…" (Dawn, s3://chip-raw/news/…)*.

### 4.2 Lagged climate contributors (spine walk)

```cypher
// Climate anomalies in the 1–3 epi-weeks preceding the flagged week, same district
MATCH (do:DiseaseObs {disease_code:'DENGUE', pcode:$pcode, epi_week_id:$week})-[:DURING]->(w:EpiWeek)
MATCH (wprev:EpiWeek)-[:PRECEDES*1..3]->(w)
MATCH (co:ClimateObs)-[:DURING]->(wprev), (co)-[:IN_LOCATION]->(:Location {pcode:$pcode})
MATCH (co)-[:OF_VARIABLE]->(v:ClimateVariable)
RETURN wprev.epi_week_id AS lead_week, v.variable_code AS variable,
       co.value AS value, co.anomaly_z AS z_score
ORDER BY lead_week;
```

### 4.3 Media early-warning lead

```cypher
// Did media chatter precede the official notification?
MATCH (ms:MediaSignal {signal_type:'dengue_mention', pcode:$pcode})-[:DURING]->(mw:EpiWeek)
MATCH (do:DiseaseObs {disease_code:'DENGUE', pcode:$pcode})-[:DURING]->(ow:EpiWeek)
WHERE (mw)-[:PRECEDES*1..4]->(ow) AND ms.intensity > ms.baseline * 2
MATCH (ms)-[:REPORTED_IN]->(doc:Document)
RETURN mw.epi_week_id AS signal_week, ow.epi_week_id AS official_week,
       ms.intensity, collect(DISTINCT {source:doc.source, uri:doc.minio_uri})[..5] AS docs;
```

### 4.4 Evidence completeness audit (governance)

```cypher
// Any substantive claim missing evidence = a provenance violation → must fail QA
//
// REVISED 2026-07-13: the `domain_prior` exemption is REMOVED. Under §0.2 every curated
// mechanistic prior cites a peer-reviewed paper as its :Evidence, so there is no longer any
// class of assertion allowed to exist without evidence. "Every claim carries evidence, no
// exceptions" is a strictly stronger and simpler invariant than "every claim except these".
MATCH (a:Assertion)
WHERE a.status IN ['hypothesized','statistical','established']
  AND NOT (a)-[:SUPPORTED_BY]->(:Evidence)
RETURN a.assertion_id, a.assertion_type, a.derivation, a.model_version;
```

---

## 5. GNN layer

### 5.1 Honest assessment — what graph ML actually buys us

At this scale (tens of thousands of nodes, low-millions of edges, ~150 districts × dozens of diseases/variables × weekly), **classical epidemiological stats are the production baseline, not GNNs.** DLNM/GLM lag-exposure models and LSTM/Prophet forecasts (already in the methodology) are interpretable, well-understood by reviewers, and sufficient for the core early-warning signal. Graph ML earns its keep only where relational structure adds signal that tabular models miss:

- **Cross-district spillover / propagation** — disease risk diffusing along adjacency and mobility edges (a district's risk depends on neighbors), which panel GLMs ignore.
- **Sparse-association discovery** — link prediction to *suggest* climate–disease associations under-represented in a given district by borrowing strength across the graph.
- **Heterogeneous fusion** — jointly embedding disease, climate, hazard, and media nodes.

**Verdict:** GNNs are **research-track (student theses)**, producing *candidate* claims that feed the graph as clearly-labelled `status:'statistical'` assertions. The **production alert** stays driven by the validated statistical models until a GNN demonstrably beats them on held-out epi-weeks.

### 5.2 Recommended first tasks

| Task | Track | Formulation | Output back into graph |
|------|-------|-------------|------------------------|
| **T1: Climate–disease link prediction** | research | edge/link prediction on a `(Disease)–(ClimateVariable)` bipartite projection with FastRP/GraphSAGE features | `Assertion{ASSOCIATED_WITH, status:'statistical', derivation:'link_prediction'}` with score |
| **T2: Temporal risk propagation** | research→production candidate | temporal GNN (e.g. T-GCN / EvolveGCN / A3T-GCN in PyG-Temporal) over the district-adjacency graph, node features = climate/disease/media per epi-week, target = next-week disease incidence | `AlertRisk` candidate + `Assertion` with model provenance |
| **T3: Node classification** | research | classify district×week nodes into risk tiers | risk-tier property on `DiseaseObs` (candidate) |

Start with **T1** (simplest, self-contained thesis) and **T2** (highest operational upside).

### 5.3 Export path: Neo4j → PyTorch Geometric

Do **not** train inside Neo4j. Export a snapshot to PyG:

```
Cypher query ──► pandas edge/node frames ──► id remap ──► torch tensors ──► HeteroData
```

```python
# sketch: build a HeteroData snapshot for epi-weeks <= cutoff
import torch
from torch_geometric.data import HeteroData
from neo4j import GraphDatabase

def export_snapshot(driver, cutoff_week):
    with driver.session() as s:
        # node features: district × epi-week observation vectors
        obs = s.run("""
          MATCH (do:DiseaseObs)-[:IN_LOCATION]->(l:Location), (do)-[:DURING]->(w:EpiWeek)
          WHERE w.epi_week_id <= $c
          OPTIONAL MATCH (co:ClimateObs {pcode:l.pcode, epi_week_id:w.epi_week_id})
          RETURN l.pcode AS pcode, w.epi_week_id AS week,
                 do.case_count AS cases, do.incidence AS inc,
                 co.value AS climate, co.anomaly_z AS z
        """, c=cutoff_week).data()
        # district adjacency (spatial edges)
        adj = s.run("""
          MATCH (a:Location)-[:ADJACENT_TO]->(b:Location)
          RETURN a.pcode AS src, b.pcode AS dst
        """).data()
    data = HeteroData()
    # ... id remap pcode->contiguous idx, stack features into tensors,
    #     data['district'].x = ...; data['district','adj','district'].edge_index = ...
    #     temporal slices keyed by EpiWeek for PyG-Temporal
    return data
```

- **Snapshotting:** export per epi-week cutoff → a sequence of graph slices (the `EpiWeek` spine gives the ordering for free — §1.5).
- **Features:** climate values + anomalies, disease incidence, media intensity, hazard flags; canonical nodes contribute type embeddings.
- Keep the **id remap table** (pcode/epi_week ↔ tensor index) so predictions can be written back to the exact nodes.

### 5.4 Training cadence & write-back with provenance

- **Cadence:** batch retrain **monthly** (or per-season) on the accumulated panel; the graph updates weekly but GNN retraining weekly is unnecessary and unstable at this scale. Version every run as a `RiskModel {model_run_id, arch, git_sha, trained_at, train_window, metrics}`.
- **Write-back contract (non-negotiable):** a prediction never becomes a bare edge. It becomes an `Assertion`/`AlertRisk` with:

```cypher
MERGE (m:RiskModel {model_run_id:$run})
  ON CREATE SET m.arch='A3T-GCN', m.git_sha=$sha, m.trained_at=datetime(),
                m.train_window=$win, m.auc=$auc
WITH m
UNWIND $preds AS p
  MERGE (a:Assertion {assertion_id:p.assertion_id})   // deterministic id incl. model_run
    SET a.assertion_type='ASSOCIATED_WITH', a.status='statistical',
        a.confidence=p.score, a.derivation='link_prediction', a.model_version=$run
  MERGE (m)-[:PRODUCED]->(a)
  WITH a,p MATCH (d:Disease{icd10:p.disease}), (v:ClimateVariable{variable_code:p.var})
  MERGE (a)-[:SUBJECT]->(v) MERGE (a)-[:OBJECT]->(d);
```

- Predictions are thus **dated, model-versioned, and queryable** ("show all claims from model X") and can be **retracted** by tombstoning that `RiskModel`'s assertions — full auditability (ADR-005).

### 5.5 Research-track vs production-track boundary

- **Research-track (theses):** T1/T3, novel temporal-GNN architectures, offline benchmarking vs GLM/LSTM. Lives in a separate `research` schema/label namespace; writes `status:'statistical'` claims that do **not** raise operational alerts.
- **Production-track:** validated GLM/DLNM + LSTM/Prophet raise `AlertRisk`. A GNN graduates to production **only** after beating the baseline on rolling-origin held-out epi-weeks, signed off in a model card.

---

## 6. Graph-RAG

### 6.1 Retrieval → generation architecture

```
   User question (EN/UR)
        │
        ▼
  ┌───────────────┐   entities/intent (district, disease, week, "why")
  │ Query planner │──────────────────────────────────────────────┐
  └──────┬────────┘                                               │
         │                                                        ▼
         │ (A) STRUCTURED SUBGRAPH RETRIEVAL             (B) SEMANTIC RETRIEVAL
         ▼                                                        ▼
  ┌──────────────────────────┐                    ┌──────────────────────────────┐
  │ Neo4j: templated Cypher  │                    │ pgvector: top-k document      │
  │ (§4 explainability trace)│                    │ chunk embeddings (cosine)     │
  │ → alert, signals,        │                    │ over normalized doc text,     │
  │   assertions+status,     │                    │ filtered to retrieved doc_ids │
  │   Evidence (quotes,URIs) │                    │ + district/time facets        │
  └──────────┬───────────────┘                    └───────────────┬──────────────┘
             └───────────────┬────────────────────────────────────┘
                             ▼
                 ┌───────────────────────────┐
                 │ Context assembler         │  dedup, rank, budget tokens,
                 │ → numbered evidence cards  │  each card = {id, quote, source, uri, status}
                 └────────────┬──────────────┘
                              ▼
                 ┌───────────────────────────┐
                 │ Self-hosted LLM (vLLM)    │  answer-only-from-context,
                 │ EN/UR, mandatory [E#] cites│  temperature low
                 └────────────┬──────────────┘
                              ▼
                 ┌───────────────────────────┐
                 │ Citation verifier (post)  │  every claim maps to a cited card;
                 │ drop/flag unsupported spans│  every [E#] exists → else regenerate
                 └───────────────────────────┘
```

- **(A)** grounds the *reasoning skeleton* (which signals, what epistemic status) via the §4 Cypher — this is what makes summaries auditable.
- **(B)** grounds the *prose evidence* — pgvector over document chunks (the analytical core already holds embeddings per ADR-003), restricted to the `doc_id`s the subgraph surfaced, so retrieval stays on-topic and every chunk is citable back to a MinIO object.
- Context cards carry `status`, so the LLM can and must distinguish "observed" from "hypothesized".

### 6.2 Self-hosted LLM recommendation (verified July 2026)

For **1–2 workstation GPUs** (assume RTX 4090-class 24 GB, or RTX A6000-class 48 GB):

| Role | Model | Why | Serving |
|------|-------|-----|---------|
| **Workhorse (EN + code + structured)** | **Qwen3-14B** (24 GB) or **Qwen3-32B** quantized to ~Q4/FP8 (fits 24 GB; comfortable at 48 GB) | Best quality-per-VRAM in 2026 consensus; strong instruction following, multilingual, huge ecosystem of quants | **vLLM** (OpenAI-compatible, high-throughput, paged KV cache) |
| **Multilingual fallback** | **Gemma 3 12B / 27B** | 140+ languages incl. Urdu, 128K context, good long-context grounding | vLLM |
| **Urdu-heavy generation** | **Qalb** (Urdu SOTA, continued-pretrained on Llama-3.1-8B) or Gemma 3 for balanced EN/UR | Purpose-built Urdu quality for Urdu-first policy briefs | Ollama (dev) / vLLM (prod) |

**Serving decision:** **vLLM** for production (throughput, OpenAI API, concurrent dashboard users, batching, structured-output/grammar support for enforced citation format). **Ollama** only for a student's laptop/dev loop. Enable **structured/JSON-guided decoding** in vLLM to force the citation schema.

> **⚠️ Superseded by ADR-014 (2026-07-13).** The original text ("on a single 24 GB card run one model; on 48 GB host Qwen3-32B") is replaced by a fixed two-device split:
>
> | Device | Hosts | Duty |
> |---|---|---|
> | **GPU 0 — serving** | XLM-R NER/RE/causal + **BGE-M3** embedder (Triton) + **the graph-RAG LLM at 7–14B AWQ** (vLLM). ~15–18 GB. | **Resident. Never preempted.** This is what lets the dashboard stay live *while* the pipeline runs. |
> | **GPU 1 — batch** | The **30–32B class as an offline *teacher*** (distillation, causal fallback, pre-annotation), XLM-R fine-tuning, the historical enrichment backfill, GNN training. | Scheduled, preemptible, owns the card. |
>
> **The big model is the teacher, not the server.** A 32B interactive RAG model would have to share a card with the encoders and would be evicted by every fine-tune. A 7–14B model serves cited summaries perfectly well at this task (the evidence cards do the heavy lifting — see §6.3/§6.4), and the 32B's quality-per-token is worth far more in the offline distillation loop that trains the online encoders (03 §1.3).

> **Embeddings for pgvector: `BAAI/bge-m3`, dense, 1024-dim (ADR-011)** — one model for both NLP document embeddings and RAG chunk embeddings. `rag_chunk.embedding` is `VECTOR(1024)`; `embed_model` + `embed_version` are stored per chunk so a model change is a **versioned re-embed batch**, never an in-place dimension change (old and new vectors cannot share an HNSW index).

### 6.3 Prompt template with citation enforcement

```
SYSTEM:
You are CHIP's climate–health analyst assistant for Pakistan. You write for policy
stakeholders (NIH, NDMA, Ministry of Climate Change). Rules:
1. Answer ONLY from the EVIDENCE cards below. If the evidence is insufficient, say so
   explicitly: "Insufficient evidence in the knowledge graph to answer." Do NOT use prior
   knowledge or speculate.
2. Every factual sentence MUST end with one or more citations like [E3] referencing the
   card id(s) that support it.
3. Distinguish epistemic status: facts marked status=observed are established; status=
   statistical are model-estimated associations; status=hypothesized are UNCONFIRMED and
   must be worded as "a hypothesized/possible link, based on reporting" — never as fact.
4. Output language: {LANGUAGE}.  Keep it concise and decision-oriented.

EVIDENCE:
[E1] (status=observed, source=NIH IDSR, s3://chip-raw/idsr/2026-w27.pdf)
     "District X reported 34 confirmed dengue cases in epi-week 2026-W27, up from 9..."
[E2] (status=statistical, model=risk-glm-2026w27, conf=0.73)
     "Total precipitation anomaly (+2.1 SD) 2 weeks prior is positively associated with
      dengue incidence in X."
[E3] (status=hypothesized, source=Dawn, conf=0.62, s3://chip-raw/news/dawn-...html)
     "Heavy monsoon rains flooded low-lying areas of X, leaving stagnant water..."
...

QUESTION: Why is dengue risk elevated in District X this week?

ANSWER (with [E#] citations, {LANGUAGE}):
```

### 6.4 Hallucination guardrails

1. **Answer-only-from-context** system rule + low temperature (≤ 0.3).
2. **Mandatory citation schema**, enforced with vLLM guided decoding (regex/grammar requiring `[E#]` tokens on factual sentences).
3. **Post-hoc citation verifier** (deterministic code, not the LLM): parse every `[E#]`, assert it exists in the card set; for each factual sentence, require ≥1 citation; optionally re-check that the cited card's `quote` is textually/semantically consistent with the sentence (embedding-similarity threshold). On failure → regenerate once, then fall back to a template ("The system found the following signals: …") that lists cards verbatim.
4. **Status honesty check:** flag any sentence that states a `hypothesized` card as fact (heuristic: causal verbs without hedging near an `[E#]` whose card is `status=hypothesized`).
5. **No-evidence path:** if retrieval returns nothing above threshold, return the explicit insufficiency message — never let the model free-generate.

### 6.5 Urdu / English output

- Language is a request parameter; default mirror the question's language (detect EN/UR).
- Same evidence cards, `{LANGUAGE}` swapped in the prompt. Prefer Gemma 3 or Qalb for Urdu fluency; keep **citations and source URIs identical** across languages (only prose is translated).
- Numerals, district names (PCODEs), and disease terms use a bilingual glossary to keep terminology consistent; the disease/location canonical nodes already store Urdu aliases (§2.4).

### 6.6 Evaluation of summary faithfulness

| Dimension | Method |
|-----------|--------|
| **Citation coverage** | % factual sentences with a valid, existing citation (automatic, target ≥ 0.95) |
| **Attribution correctness (faithfulness)** | each sentence entailed by its cited card — LLM-as-judge (a *different* served model) + human spot-check on a labeled set; report entailment/contradiction rates |
| **Status fidelity** | audit that no `hypothesized` card is presented as fact (rubric scored) |
| **Retrieval sufficiency** | recall of gold evidence for a curated question set (does the subgraph+pgvector retrieve the docs an analyst would cite?) |
| **Answerability calibration** | on questions with no supporting evidence, does it correctly abstain? |
| **Bilingual parity** | same question EN vs UR yields consistent facts/citations |

Maintain a **gold Q&A/evidence set** (curated with the domain-orientation partners) as a regression harness run on every LLM/prompt/model change.

---

## 7. Open questions

### Closed since the 2026-07-13 review

| Was | Now |
|---|---|
| ~~OQ-1 District boundary versioning~~ | **Closed by ADR-015.** Subsystem 01 §1.2/§1.4 already specified SCD-2 `dim_location` + `location_lineage`. `Location.pcode` nodes are built **per boundary vintage**; the CHKG declares the `analysis_vintage` it was projected onto. Adjacency for the temporal GNN (OQ-3) is derived per vintage too. |
| ~~OQ-2 Epi-week convention~~ | **Pinned by ADR-008** (WHO/ISO Monday-start, parameterized, validation-gated). Not this subsystem's decision. |
| ~~OQ-10 Evidence text storage / offset rot~~ | **Escalated into ADR-013 contract C2**, where it belongs: the normalized text now lives in **NAaaS**, so *NAaaS* must guarantee stable `doc_id`s, a content hash, and byte-identical retrievability for 3+ years, with re-extraction published as a **new version** rather than mutating the old. **If NAaaS cannot guarantee this, CHIP re-archives full text into its own bronze** — offsets rotting under our evidence spans is not survivable for an auditability-first system. |

### Still open

1. **`ADJACENT_TO` source.** T2 (propagation GNN) needs district adjacency (ideally mobility). Derive adjacency from the COD-AB polygons in PostGIS (`ST_Touches`) — **per boundary vintage** (ADR-015). Is any mobility proxy (roads, census flows) obtainable?
2. **Causal claim policy.** How aggressive should NLP `TRIGGERS` extraction be? Precision-first (few, high-confidence hypotheses) vs recall-first (more candidates, more curation). Ties to the entity-resolution review budget. Joint decision with 03 (OQ-4 there).
3. **Confidence fusion.** When an `Assertion` accumulates multiple `Evidence` links, how do we combine per-evidence extraction confidences into an effective claim confidence (noisy-OR? capped count?) without overstating certainty? **Note the wire-copy hazard:** if the same syndicated story is counted as three independent evidences, confidence inflates on what is really one source. Depends on cross-outlet clustering (03 §6.1 stage [2] / 02 OQ-5).
4. **Backup window feasibility.** Is a nightly Neo4j stop acceptable, or do we need an LVM/ZFS snapshot of the stopped store to shrink the window? Depends on dashboard usage hours.
5. **GDS vs PyG split.** Which embeddings (FastRP/node2vec) are cheap enough to compute *in* Neo4j GDS (≤ 4-core CE cap) vs must move to PyG? Benchmark once the first full graph is loaded.
6. **Academic Enterprise license.** Worth applying for Neo4j's education/research Enterprise license now (unlocks online backup, node-key constraints, GDS concurrency) as a zero-cost hedge?
7. **LLM licensing for a government deliverable.** Confirm the chosen open-weight model's license (Qwen3 / Gemma 3 terms, Qalb's base-model lineage) permits public-sector deployment; document it in the model card.
8. **⭐ NEW — who authors the mechanistic prior ontology, and is an epidemiologist available to validate it? (§0.2)** Without it the graph cannot answer "why", and the §0.1 gate fails by default. This is the **highest-priority open question in this subsystem** — it is a prerequisite for the thing that justifies Neo4j's existence.
9. **⭐ NEW — does the §0.1 gate pass?** Ten analyst questions, Cypher vs SQL, before Phase 3. If most are SQL-shaped, **cut Neo4j to the RAG evidence layer** and keep facts in Postgres. One student-week; do not skip it.
```

