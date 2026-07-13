# CHIP — Decisions, Alternatives, Pros & Cons (single-file guide)

**Climate–Health Intelligence Platform · PCN / NUCES-FAST Islamabad · NRPU**
*Created 2026-07-10. Companion to `00-ARCHITECTURE-OVERVIEW.md`, the ADRs in `adr/`, and the subsystem docs in `subsystems/`.*

This file explains, in one place, **what has been decided, what the alternative routes were, and the trade-offs**. It has four parts:

- **Part A — Foundational decisions (ADR-001…005):** the platform shape, set before the subsystem docs.
- **Part B — Reconciliation decisions (ADR-006…011):** the shared *contracts* that the seven subsystem docs disagreed on. These were pinned on 2026-07-10 during an architecture-review pass and each supersedes the conflicting text in the subsystem docs it names.
- **Part D — Reality-check decisions (ADR-012…015):** ⭐ **new, 2026-07-13.** Four *premises* in the original design turned out to be false. These ADRs replace them. See [`CRITIQUE-AND-OPEN-ISSUES.md`](CRITIQUE-AND-OPEN-ISSUES.md) for the analysis.
- **Part C — Still-open questions** that a decision could not close yet (they depend on external facts — NIH/PMD/NAaaS/NUCES answers).

A one-line reading of the whole thing: **CHIP is a hard-integration problem, not a big-data problem** (~160 districts, weekly bulletins, **~0.5–1 TB lifetime** — see the correction in Part D), and the dominant risk is **team turnover and operational entropy**. Almost every decision below optimizes for *correctness, auditability, and a rotating-student team* over throughput or novelty.

---

## How to read the trade-off tables

Each decision lists the **chosen** route first, then rejected alternatives. "Pros/Cons" are relative to *this* project's constraints (small data, self-hosted, rotating MS/PhD team, government stakeholders), not in the abstract.

---

# Part A — Foundational decisions

## ADR-001 — Service topology: event-driven pipeline + one modular-monolith serving app

**Decision.** 5–7 coarse-grained deployables around a Kafka backbone (connectors → normalizers → enrichers → storage zones → KG/panel), plus **one** modular-monolith FastAPI app (API + auth + alerts + RAG serving + dashboard backend) with enforced internal module boundaries. Unit of decomposition = *data domain + pipeline stage*, not business capability.

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **Event-driven pipeline + modular monolith (chosen)** | Failure isolation and independent scheduling for very different workloads (scrapers vs GPU inference vs public dashboard); a student can own one stage end-to-end; new source = new plugin | Kafka becomes a hard inter-stage dependency | **Chosen** — matches the workload heterogeneity without microservice tax |
| Full microservices | Independent scaling/teams | Tracing, discovery, network failure modes — unaffordable ops tax for a rotating student team; solves problems CHIP doesn't have | Rejected |
| Single monolith | Simplest deploy | Can't isolate failures or schedule scrapers/GPU/dashboard independently | Rejected |

## ADR-002 — Kafka everywhere; Spark only where it earns its keep

**Decision.** **Kafka carries all sources** (satisfies the proposal's deliverables). **Spark is scoped to two jobs only:** the news NLP enrichment stream and historical backfills. All low-frequency sources (IDSR weekly, PMD daily, NDMA event-driven) are plain Python producers/consumers orchestrated by **Dagster**.

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **Kafka-all + Spark-for-news-only (chosen)** | Keeps every proposal/deliverable claim literally true; only the NLP team needs Spark skills; ingestion students write ~50-line Python | Two processing styles coexist (mitigated by shared libs) | **Chosen** |
| Everything through Spark Streaming | Maximal fidelity to "Spark" wording | Distributed streaming engine on one weekly PDF is pure ceremony; forces Spark on everyone | Rejected |
| Kafka for news only | Marginally less to run | Contradicts the deliverables list (Kafka producers for all four source families) | Rejected |

*Status nuance:* ADR-002 and ADR-003 are still marked **"Proposed (default in effect)"** — they drive every subsystem but await the project lead's explicit sign-off. Worth formally accepting.

## ADR-003 — Storage: Postgres-centric + Neo4j + MinIO, nothing else

**Decision.** Three systems total: **PostgreSQL** (PostGIS + TimescaleDB + pgvector + native FTS) as the analytical core; **MinIO** (S3-compatible) as the immutable bronze/raw store; **Neo4j Community** for the knowledge graph. Cross-domain analysis (weather × cases × media) is SQL joins on one conformed grain in one database.

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **Postgres + MinIO + Neo4j (chosen)** | One backup/restore story for the core; one connection string for students; cross-domain joins stay in-database | Postgres FTS may not scale to millions of Urdu articles (documented escape hatch: scoped OpenSearch later) | **Chosen** |
| Lakehouse-first (Delta/Iceberg on MinIO) | Scales to ~1000× our data | Adds table format + catalog + forces Spark into every transform (contradicts ADR-002) | Rejected — pays off far above our volume |
| Polyglot best-of-breed (Timescale + OpenSearch + Qdrant + Neo4j + MinIO) | Each tool best-in-class | Five engines to babysit for 3+ yrs; splits data so cross-domain joins move into app code | Rejected |

## ADR-004 — Self-hosted, cloud-agnostic infrastructure

**Decision.** Everything self-hosted on NUCES hardware, containerized. **Docker Compose first**, migrate to **k3s only on explicit triggers** (07 §3.3). No managed cloud services in the critical path; cloud-portable by construction (S3 API, standard Postgres, containers). Stakeholders reach only the dashboard/API via reverse proxy + TLS.

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **Self-hosted, Compose-first (chosen)** | Matches the institutional constraint (full control); cheapest ops model for the team; lift-and-shift later if approved | Team owns backups/monitoring/TLS/capacity; hardware is a procurement dependency | **Chosen** (institutional constraint) |
| Managed cloud (RDS/EKS/managed Kafka) | Less ops | Violates the "full control / self-host" institutional decision; recurring cost | Rejected |
| Kubernetes from day one | Zero-downtime, scheduling | Complexity a rotating student team can't sustain before it's needed | Deferred to k3s triggers |

## ADR-005 — Canonical grain (district × epi-week) + mandatory provenance

**Decision.** Canonical spatial key = district **P-code** from a versioned gazetteer; canonical temporal key = **epi-week**; **every derived record carries provenance** (`source_id`, `retrieved_at`, `raw_object_uri`, `transform_version`), and KG edges reify evidence. The gazetteer and epi-week libraries are Phase-0 deliverables.

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **District × epi-week + mandatory provenance (chosen)** | The operationally relevant surveillance resolution; auditability is structural (traceable to source docs), not procedural; enables clean replay | Requires disciplined gazetteer + epi-week libs before anything downstream | **Chosen** — follows directly from proposal commitments |
| Finer grain (tehsil/UC) | More spatial detail | Sources don't reliably report it; risks re-identification (ethics); no stable historical series | Rejected for v1 (retained as future drill-down) |
| No hard provenance mandate | Less plumbing | Kills the "explainable/auditable for policy" requirement | Rejected |

---

# Part B — Reconciliation decisions (the contracts the subsystems disagreed on)

> These six ADRs are the substance of the "do it fully" request. Before them, the seven subsystem docs were individually excellent but **disagreed on shared contracts**, which would have caused real integration failures. Each ADR pins the seam to one answer.

## ADR-006 — One canonical spatial key: COD-AB district P-code

**The conflict.** Three code spaces were in use for the *same* districts: COD-AB P-codes (`PK101`) in 01/06, **GADM 4.1** ids (`PAK.8.14_1`) in the NLP geo-linker (03), and a `pcode` labelled "**PBS/HDX** admin code" in the CHKG (04); analytics (05) used an opaque integer. Since the geo-linker feeds the CHKG and the panel, that's a broken join.

**Decision.** The single key is the **OCHA/HDX COD-AB `admin2` P-code**. GADM, GeoNames, PBS census codes, PMD station registry, NIH reporting-unit lists are **alias/enrichment sources only**, crosswalked *into* P-codes — never keys. The NLP linker emits `pcode`; the CHKG `Location.pcode` is the COD-AB P-code; analytics `district_id` is (or maps 1:1 to) the P-code.

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **COD-AB P-codes (chosen)** | The humanitarian/government standard; ships geometries + a tabular alias source; **aligns to NDMA/NIH/PDMA** code space; stable P-codes with a versioning story | P-code strings are opaque (join through parent columns, don't parse digits) | **Chosen** |
| GADM 4.1 as canonical | Rich alternate names, global coverage | ids **not stable across GADM versions**; not the standard our stakeholders use; we'd translate away from our own users | Rejected (kept as alias source) |
| PBS census codes | Officially Pakistani | No clean open gazetteer with geometries/aliases; re-versions each census | Rejected (crosswalk only) |
| geoBoundaries | Good open geometries | COD-AB is the de-facto standard for this exact context and ships the alias table we need | Rejected |

## ADR-007 — One Kafka wire contract: CloudEvents + JSON Schema + versioned topic names

**The conflict.** Three envelope formats (01's **CloudEvents 1.0**, 02's flat custom envelope, 03's implicit form) and three topic-naming schemes (`chip.<domain>.<source>.<entity>.v1` vs `chip.raw.<source>` vs `raw.news.dawn`). You cannot run three of each on one bus.

**Decision.** Subsystem 01 §3 wins: **CloudEvents 1.0** structured JSON envelope (provenance in `data.provenance`), **JSON Schema** serialization, topic naming `chip.<domain>.<source>.<entity>.v<major>` (a breaking schema change = a new topic). The ingestion SDK's `Provenance` object is kept as the in-process form and serialized into the envelope.

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **CloudEvents + JSON Schema + `chip.<domain>…v<major>` (chosen)** | Standard tracing/extension attributes for free; versioned topics enable blue/green schema cutover; JSON is inspectable in `kafka-console-consumer`/logs (huge onboarding win); Postgres is JSON-native | Slightly more verbose than a bespoke envelope | **Chosen** |
| 02's flat custom envelope | Minimal, readable | Reinvents a subset of CloudEvents, loses tooling; two standards = entropy | Rejected |
| `chip.raw.<source>` topic naming | Groups by pipeline zone | No domain/entity/version in the name → breaking changes can't be a clean new topic | Rejected |
| Avro / Protobuf | Compact, strong typing | Binary, non-debuggable; unjustified at low-thousands msgs/day | Rejected (documented Phase-2 option) |

## ADR-008 — Epi-week convention: WHO/ISO (Monday-start), validation-gated

**The conflict.** 01/ADR-005 defaulted to **WHO/ISO (Monday)**; analytics (05) modelled on **MMWR (Sunday)**; 04 left it open. NIH bulletins print "Week N" with no dates, so nobody has *verified* it — and a wrong week-start silently misaligns every join at year boundaries.

**Decision.** Platform default **WHO/ISO Monday-start** (Pakistan IDSR runs under WHO/EMRO). `libs/epiweek` is parameterized (`iso`/`cdc`), canonical key `epiweek_id = year*100 + week`. **Lock only after** empirically reconciling known `(week, year)` labels against dated NIH/WHO-EMRO material (OQ-1); if it turns out MMWR, flip one config flag.

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **WHO/ISO Monday, validation-gated (chosen)** | Matches Pakistan's WHO/EMRO surveillance context; both systems built in, so a late flip is one line; protects 10 yrs of backfill from encoding the wrong start | Can't freeze `dim_epiweek` until OQ-1 is answered | **Chosen** |
| MMWR/CDC Sunday as default | US IDSR tradition; `epiweeks` supports it | Likely misaligns to actual NIH bulletins | Rejected as default (kept as switchable fallback) |
| Freeze a convention now, skip the check | Faster | Risks silently baking in the wrong week-start — expensive to unwind | Rejected |

## ADR-009 — Schema registry: Apicurio (Postgres-backed)

**The conflict.** 01 chose **Apicurio**; 07 assumed **Karapace** everywhere (sizing, backups). Different backup and rebuild stories.

**Decision.** **Apicurio**, Apache-2.0, schemas persisted in the **main Postgres** — so it rides the existing pgBackRest backup and needs **no** separate export job. Confluent-compatible REST API; per-subject BACKWARD compatibility checked in CI.

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **Apicurio / Postgres-backed (chosen)** | Reuses an engine we already back up (fewer stateful services — 07's own principle 4); Apache-2.0; standard SerDes | One more schema table set in Postgres | **Chosen** |
| Karapace | 1:1 Confluent API, tiny | Stores schemas in a **compacted Kafka topic** = new durable bus state needing a separate nightly export to be rebuildable | Rejected |
| Confluent Schema Registry | Reference implementation | Confluent Community License + telemetry; we keep the stack Apache-2.0 | Rejected |

## ADR-010 — KG consumption contract: reified assertions, no raw semantic edges

**The conflict.** 04 forbids direct causal edges (every causal/statistical link is a reified `:Assertion` node with epistemic `status` + evidence). But the serving doc (06) modelled alert evidence as a direct `(:HazardEvent)-[:INCREASES_RISK_OF {confidence}]->(:Disease)` edge — a shape the KG builder never emits, and one that silently states a hypothesis as fact.

**Decision.** **Definitional edges** (`IN_LOCATION`, `DURING`, `OF_DISEASE`, `PRECEDES`, `PARENT_OF`, …) are plain edges. **Semantic links** are `:Assertion` nodes read via `SUBJECT`/`OBJECT`/`SUPPORTED_BY` with `status`, `confidence`, `model_version`. Serving exposes an assertion-shaped view; the dashboard **must render epistemic status** (observed vs statistical vs hypothesized).

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **Reified assertions only (chosen)** | Attaches multiple evidence + model version + status to each claim; auditability is structural (ADR-005); never asserts causation as fact | One extra hop in Cypher (free at this graph scale) | **Chosen** |
| Raw typed edges with `confidence` (06's sketch) | Simpler Cypher, one fewer hop | A Neo4j relationship can't carry evidence/status sub-structure; silently encodes causation; breaks the explainability commitment | Rejected |
| Hybrid (reified + denormalized fast-read edge) | Fast reads | Two representations of one truth that drift; the extra hop we'd optimize away is already negligible | Rejected |

## ADR-011 — Embedding model: BGE-M3 (1024-dim), one model for NLP + RAG

**The conflict.** pgvector column was `VECTOR(1024)` (01, bge-m3 class), while 03 named `multilingual-e5-base`/LaBSE (768-dim). A dimension mismatch = a full table rebuild + re-embed if caught late; old/new vectors can't share an HNSW index.

**Decision.** **`BAAI/bge-m3`, dense, 1024-dim**, self-hosted, for **both** NLP document/signal embeddings and RAG chunk embeddings. `embed_model`+`embed_version` stored per chunk so a model change is a versioned re-embed batch, never an in-place dim change. Confirm Urdu retrieval quality before the at-scale HNSW build.

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **BGE-M3, 1024-dim (chosen)** | Strong multilingual incl. Urdu; long context (up to 8k); multi-granularity (dense/sparse/colbert) to grow into; keeps 01's `VECTOR(1024)` valid | Heavier than e5-base; VRAM contends with the LLM/encoders (schedule accordingly) | **Chosen** |
| multilingual-e5-base (768) | Light, fast | Weaker Urdu, shorter context — and Urdu quality is the whole novelty | Rejected (keep as a benchmark comparator) |
| LaBSE (768) | Strong cross-lingual sentence alignment, 109 langs | Sentence-oriented and older; bge-m3 beats it on retrieval + context length | Rejected |

---

# Part D — Reality-check decisions (ADR-012…015)

> **Why these exist.** Parts A and B were internally consistent — but they rested on **four premises that turned out to be false.** A reconciliation pass can only align documents with each other; it cannot tell you that all of them are wrong about the world. These four ADRs replace the false premises. Full analysis in [`CRITIQUE-AND-OPEN-ISSUES.md`](CRITIQUE-AND-OPEN-ISSUES.md).

| The false premise | The reality | Fixed by |
|---|---|---|
| Institutional PDFs have a digital text layer; `pdfplumber`/`camelot` is the primary extractor | **They require an agentic parse.** The deterministic tiers were never going to be the hot path. | **ADR-012** |
| CHIP operates six per-outlet news scrapers | **News comes from the lab's own NAaaS platform** via a keyword + date-range API | **ADR-013** |
| One 24 GB GPU is enough (07) / no — a 30B model is needed (03) / no — a 32B (04) | Three docs, three machines. Resident serving *plus* batch training does not fit on one card. | **ADR-014** |
| District-boundary versioning is an unsolved open question (raised in 02, 03 **and** 04) | **Subsystem 01 §1.4 had already solved it** (SCD-2 + `location_lineage`) and nobody noticed | **ADR-015** |

## ADR-012 — Document parsing: agentic parse, cached immutably to bronze

**Decision.** Agentic parse (LlamaParse-class) is the **primary** path for institutional PDFs. **Every parse result is cached immutably in bronze**, keyed `(content_hash, parser_id@version)`. `dim_source.access_tier` gates which sources may touch a cloud parser. Deterministic extractors are demoted to cross-validators. **Numeric total-reconciliation becomes mandatory.**

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **Agentic parse + immutable bronze cache (chosen)** | Actually works on these documents; **cache restores ADR-005's determinism** (an LLM parser is not deterministic — a cached artifact is); pay per page exactly once, forever; vendor risk collapses to *new* documents only; **the cached outputs become the ground-truth eval set for an in-house replacement** | A cloud dependency for public sources; must enforce the `access_tier` gate as a hard control | **Chosen** |
| Build an in-house parser to that quality first | No vendor dependency, fully on-prem from day one | **Blocks the backfill — the highest-value task in the project — on a research problem of unknown duration.** Trades weeks of delay for ~$50. | Rejected as a *prerequisite*; kept as a parallel eval track |
| Keep `pdfplumber`/`camelot` primary, agentic as fallback | No cloud call in the common case | The premise is false: the deterministic tiers don't work, so the "fallback" is the hot path anyway — but without the caching discipline | Rejected (this is the design being superseded) |
| Re-call the parser on every replay | No cache to manage | Non-deterministic replay (breaks ADR-005), unbounded recurring cost, unbounded vendor exposure | Rejected |

> **The sharpest consequence:** an agentic parser fails *differently*. `camelot` fails by returning **nothing** — loud and safe. A VLM fails by returning a **plausible, well-formed, wrong number** — silent, and exactly the corruption that destroys institutional trust in an epidemiological platform. **Total-reconciliation is therefore the primary defence, not a safety net.** Better a quarantined bulletin (a visible backlog item) than a hallucinated case count (an invisible lie that propagates into the panel, the models, the KG, and a policy brief).

## ADR-013 — News via the NAaaS API; retire the scrapers

**Decision.** One `naaas` connector queries the lab's News-Analytics-as-a-Service API (keywords + date range, multilingual). CHIP scrapes nothing. **Requires two contracts from NAaaS: C1 an unfiltered count endpoint; C2 stable doc identity + durable text.**

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **Consume the NAaaS API (chosen)** | Reuses funded lab infrastructure — **this is the literal reading of the proposal** ("extending the validated NAaaS backbone"); **the relevance gate comes for free**; kills the acquisition wall; deletes 6 connectors and the entire scraping-legal surface | Hard dependency on a sibling project; CHIP inherits NAaaS's coverage limits; needs C1/C2 | **Chosen** |
| CHIP runs its own per-outlet scrapers (the previous design) | Full control of coverage and extraction | Duplicates a system the lab already owns; **~35 days to politely backfill one year**; 6 connectors under student turnover; a legal surface to defend | Rejected |
| Both (NAaaS primary + CHIP scrapers for gaps) | Coverage insurance | Two ingestion paths, two dedup strategies, two provenance shapes — **the exact entropy ADR-001/002 exist to prevent.** If an outlet is missing, **add it to NAaaS.** | Rejected |

> **This ADR silently fixed the largest sizing defect in the architecture.** Only ~2–5% of the news firehose is health-relevant (03 §3.1) — but the *pipeline* had no relevance gate: every article was NER'd, RE'd, geo-linked, **embedded**, and turned into a `:Document` node. A keyword-driven API means CHIP **never ingests the other 95–98%.** Without it: pgvector needs ~12 GB of vectors against a 16 GB Postgres (**it would not build**), Neo4j reaches 5–10M nodes against a design point of "tens of thousands," and a 5-year enrichment costs 2–6 GPU-**days**. With it, all three land exactly where the docs claimed. **The cost of the gate is recall** — an outbreak in unusual framing can be filtered out — so its false-negative rate must be *measured*, not assumed (03 OQ-7).

## ADR-014 — GPU allocation: 2 × 24 GB, split serving / batch

**Decision.** **GPU 0 = resident serving** (XLM-R NER/RE/causal + BGE-M3 + a 7–14B RAG LLM ≈ 15–18 GB), **never preempted**. **GPU 1 = batch/training** (30B-class *teacher*, fine-tuning, historical enrichment, GNN), owns the card, may OOM freely.

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **2 × 24 GB, serving/batch split (chosen)** | **Failure isolation**: a student's fine-tune OOM cannot touch the stakeholder dashboard; no scheduler needed (`CUDA_VISIBLE_DEVICES`); cheap (used 3090s); two independent streams | Two cards to buy, cool, and power (1200 W+ PSU) | **Chosen** |
| 1 × 24 GB, time-sliced | Cheapest; survivable in Phase 0–2 | **Breaks exactly when the historical enrichment backfill runs *and* the dashboard must be live — i.e. when you demo.** Caps the RAG model at 7–8B permanently. | Rejected as the target; acceptable as a month-0 interim |
| 1 × 48 GB (A6000-class) | One card; could host a 32B interactively | **No failure isolation** — a training OOM kills serving on the same device; needs MIG/MPS, which a rotating team won't maintain; more expensive | Rejected |
| 3+ GPUs / A100-class | Headroom | Nothing in CHIP needs it (07 says so itself); destroys the budget and power envelope | Rejected |

> **The non-obvious call: the big model is the *teacher*, not the *server*.** A 32B interactive RAG model would share a card with the encoders and be evicted by every fine-tune. A 7–14B model serves cited summaries perfectly well — **the evidence cards do the heavy lifting, not the model's parametric knowledge** — while the 32B's quality-per-token is worth far more in the offline distillation loop that trains the online encoders (03 §1.3). And **buying the parser (ADR-012) removes a GPU workload**: no local VLM is needed for documents, which is part of why two cards suffice rather than three.

## ADR-015 — Boundary versioning is settled (01 §1.4); revisions are *measured*, not assumed

**Part A — boundaries.** Three subsystems (02, 03, 04) each raised "how do we handle Pakistan's changing districts?" as an open question. **Subsystem 01 had already answered it** and nobody noticed: SCD-2 `dim_location` (`valid_from`/`valid_to`/`cod_ab_version`) + a `location_lineage` table with `area_fraction`. Facts resolve against **the boundary vintage valid on the fact's own date**; historical bulletins are never rewritten. ADR-015 pins this, closes the three OQs, and adds: **apportionment is population-weighted, not area-weighted** (cases follow people, not hectares), and **every mart and model card declares its `analysis_vintage`.**

**Part B — revisions.** 05 §2.5 built an entire bitemporal apparatus on the *assumption* that IDSR revises its counts. The project lead believes it does not. **Both cannot drive the design.**

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **Instrument the UPSERT; measure (chosen)** | Answers it with data, not belief; ~10 lines (`ingestion_revision` row whenever an incoming value differs from a stored one); **the historical backfill answers it for free**; commits us to nothing | Requires the parser to read each bulletin's full retrospective table — cheap insurance | **Chosen** |
| Assume no revisions; drop the bitemporal design | Simpler now | If wrong, **every backtest is silently optimistic and unrecoverable** — the exact failure §2.5 was written to prevent | Rejected |
| Assume revisions; build it all regardless | Safe | Significant complexity that may serve nothing — and it *still* wouldn't tell you whether it was needed | Rejected |
| "We'll find out during KG building" | No work now | **Structurally impossible.** The CHKG is built *from* the gold panel; if the normalizer overwrote the old value, the revision was destroyed **before the graph ever saw it.** The UPSERT is the only place it is observable. | Rejected |

> **The consequence nobody had stated:** if revisions don't exist, **CHIP forecasts *first-reported* counts, not truth.** That must be said out loud — 05 §3.4's outbreak gold standard is currently defined on *"revision-mature counts"*, which would then be **unbuildable**, and every stakeholder-facing number must be labelled a forecast of *reported* cases. Under-reporting is a property of the surveillance system, not a bug in the model; conflating them is how a platform loses institutional trust.

---

# Part C — Still-open questions no decision could close yet

These depend on facts only NIH / PMD / **NAaaS** / NUCES can supply. They are **not** unfinished design — they are external dependencies to chase early.

### Newly blocking (from the 2026-07-13 review)

| # | Question | Blocks | Owner |
|---|---|---|---|
| **OQ-A** | **NAaaS contract C1 — an unfiltered count endpoint.** (ADR-013) | **The media-surge denominator, i.e. the project's headline hypothesis.** Without it, `media_surge_z` is uninterpretable — a district whose papers simply publish more looks identical to one with an outbreak. Also the only reliable upstream-liveness probe. **Agree it NOW, while the API is being designed.** | Ingestion + NAaaS team |
| **OQ-B** | **NAaaS contract C2 — stable `doc_id` + byte-identical text for 3+ years.** (ADR-013) | Evidence-span offsets (04 §1.3) **rot** if upstream text mutates. If NAaaS can't guarantee it, CHIP must re-archive full text into its own bronze. | Ingestion + NAaaS team |
| **OQ-C** | **Who authors the mechanistic prior ontology, and is an epidemiologist available to validate it?** (04 §0.2) | **The KG's entire reason to exist.** Without ~50–200 cited disease-biology assertions, the multi-hop "why" traversal has nothing to traverse, and 05's alerts cannot carry the evidence bundle they promise. Lands squarely on the proposal's own stated weakness ("limited in-house epidemiology expertise"). | PI + domain partner |
| **OQ-D** | **Does the 10-question Cypher-vs-SQL gate pass?** (04 §0.1) | Whether Neo4j is built at all, or cut to the RAG evidence layer. **One student-week, before Phase 3.** | KG owner |
| **OQ-E** | **Parser billing: per page or per document?** (ADR-012) | 500 bulletins × 15–20 pages ≈ **10,000 pages, not 500 docs** — a 20× planning error. Cheap either way; get the number right. | Ingestion |
| **OQ-F** | **Relevance-gate recall.** (03 OQ-7) | The keyword query is now the only thing between CHIP and 95–98% of the firehose. **What does it miss?** An outbreak in unusual framing ("mystery illness") could be filtered out silently. Needs a labelled recall eval + a 1% sample of non-matches. | NLP |

### Carried forward

| # | Question | Blocks | Owner |
|---|---|---|---|
| OQ-1 | Does NIH IDSR use WHO/ISO or MMWR weeks? (bulletins omit dates) | Freezing `dim_epiweek`; ADR-008's default | Data-model + NIH contact |
| OQ (05.1) | **Outbreak gold-standard definition — now coupled to ADR-015.** If IDSR never revises, "revision-mature counts" doesn't exist and the current definition is unbuildable. | **Every operational metric.** Settle before freezing the eval harness. | Analytics |
| OQ (05.2) | Usable history per disease-district | The v1 forecasting scope. **This is the binding constraint on the project — it is disease-data-limited, not compute-limited.** | Analytics |
| OQ (02.1) | Is a PMD/CDPC MOU obtainable, and can its single-user licence coexist with a research platform? | Whether ERA5 reanalysis is *permanently* primary | Ingestion + PI |
| OQ (06.1 / 07.5) | NIH sensitivity tiers; may PDMAs see NIH district data? **Now also governs which sources may touch the cloud parser (ADR-012 `access_tier`).** | `institution_grants`; per-institution visibility; **cloud-parse eligibility** | PI / serving |
| OQ (07.3) | Will NUCES IT grant the 443 DNAT + WireGuard, and when? | External stakeholder access; file in month 1 | Infra lead |
| OQ (02/03) | Urdu geocoding accuracy to admin2 after confusable normalisation | Trusting news-derived spatial signals | NLP |
| OQ (05.7) | The false-alarm budget per severity — **must be agreed with NIH/NDMA**, not chosen by us | Alert operating points; alerting stays in shadow mode until set | PI / stakeholders |

### Parked by decision (not forgotten)

| # | Question | Status |
|---|---|---|
| OQ-H | **The live Punjab DHQ/THQ hospital feed** (~200 facilities, daily) | **Deliberately not designed** (00 §6.1). It is *not* big data (~8k rows/day aggregate; even patient-level is ~1–20 events/sec), the architecture absorbs it, and it would be **the first source that genuinely justifies Kafka** — plus it solves the vintage problem outright. But two non-technical questions decide it first: **(1) one provincial HMIS endpoint or 200 facility systems?** and **(2) aggregate counts or patient-level records?** — patient-level would breach the ethics envelope the proposal was approved under. **Answer those two before designing anything.** |

---

## Change log

- **2026-07-13** — **Reality-check pass.** Four false premises identified and replaced: PDF parsing (ADR-012), news acquisition (ADR-013), GPU budget (ADR-014), boundary versioning + revision detection (ADR-015). Part D added. ADR-002 amended (the "real parallel inference" claim for Spark withdrawn; Kafka's honest justification stated; **low-frequency consumers pinned as Dagster batch drains, not long-running consumers**). The `<100 GB lifetime` figure in 00 §1 corrected to ~0.5–1 TB. The `00` architecture diagram redrawn (it wrongly implied Kafka reads from MinIO). A silent-data-loss bug fixed in the connector runner (missing `producer.flush()` before the watermark advanced). Subsystem 04 gained the **Cypher-vs-SQL gate** (§0.1) and the **mechanistic prior ontology** workstream (§0.2); subsystem 05 gained **alert triage** (§5.4.1). Residual ADR-006/009/011 leftovers cleaned out of 03 and 07. Analysis: `CRITIQUE-AND-OPEN-ISSUES.md`.
- **2026-07-10** — Initial version. Foundational ADRs 001–005 summarized; reconciliation ADRs 006–011 authored to close six cross-subsystem contract conflicts found in architecture review; subsystem docs annotated at each contradiction point; this guide written. See `adr/006`…`adr/011` for the full decisions and `00-ARCHITECTURE-OVERVIEW.md` §3 for the index.
