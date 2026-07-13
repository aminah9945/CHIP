# CHIP — Architecture Critique, Round 2

*Written 2026-07-13, in response to a challenge from the project lead. Source of truth = `Project Overview-20260710115307.md` (the NRPU proposal). This document critiques `00-ARCHITECTURE-OVERVIEW.md`, the ADRs, and subsystems 01–07.*

> ## ✅ STATUS — resolved into the architecture, 2026-07-13
>
> This critique has been **acted on**. The project lead supplied four facts that changed the picture, and the findings below are now closed by four new ADRs. **Read this document as the *reasoning*; read the ADRs as the *decisions*.**
>
> | Finding | Outcome |
> |---|---|
> | **F1** — no relevance gate on the news pipeline | ✅ **Dissolved by [ADR-013](adr/013-news-via-naaas.md).** News comes from the lab's **NAaaS API** (keyword + date range), so the gate exists *by construction* — CHIP never ingests the irrelevant 95–98%. The **cost** of the gate (recall — an outbreak in unusual framing could be filtered out silently) is now tracked as 03 OQ-7. |
> | **F2** — the "<100 GB lifetime" premise is false | ✅ **Corrected** to ~0.5–1 TB in `00 §1.1` (with a breakdown) and `07 §0`. The *conclusion* — not a big-data problem — survives. |
> | **F3** — no OCR path; PDFs assumed to have a text layer | ✅ **Confirmed as a real defect and fixed by [ADR-012](adr/012-document-parsing.md).** The documents require an **agentic parse**. The `pdfplumber → camelot` primary path is superseded; every parse result is **cached immutably to bronze** (which is what restores ADR-005's determinism, caps the cost at one call per document forever, and collapses vendor risk). |
> | **F4** — boundary versioning unowned, raised 3× | ⚠️ **I was partly wrong.** Subsystem **01 §1.2/§1.4 had already solved it** (SCD-2 `dim_location` + `location_lineage`); the real defect was that 02/03/04 didn't know. ✅ **[ADR-015](adr/015-boundary-versioning-and-revisions.md)** pins the seam and closes all three OQs. |
> | Mechanistic prior ontology — no author, no workstream | ✅ **Now a funded deliverable**: `04 §0.2`, `ontology/priors.yaml`, ~50–200 **cited** assertions. Tracked as **OQ-C** (blocking). |
> | `producer.flush()` bug in the connector runner | ✅ **Fixed** in `02 §1.3` — silent data loss on crash. |
> | KG justification is the thinnest in the stack | ✅ **`04 §0.1` — the 10-question Cypher-vs-SQL gate**, one student-week, before Phase 3. |
> | IDSR revisions ("we'll find out during KG building") | ✅ **[ADR-015 Part B](adr/015-boundary-versioning-and-revisions.md).** You *can't* find out at the KG — it reads the panel, which already overwrote the evidence. `ingestion_revision` instruments the **normalizer UPSERT** instead. ~10 lines; the backfill answers it for free. |
> | GPU budget sized three ways in three docs | ✅ **[ADR-014](adr/014-gpu-allocation.md)** — 2 × 24 GB, GPU0 serving / GPU1 batch. Closes 03 OQ-9. |
> | Spark's "real parallel inference" justification | ✅ **Withdrawn** in ADR-002 §Amendment; replaced with the honest case. Low-frequency Kafka consumers pinned as **Dagster batch drains**. |
>
> **Still parked:** the live Punjab hospital feed — recorded in `00 §6.1`, deliberately not designed until two non-technical questions are answered (one HMIS endpoint or 200 systems? aggregate counts or patient-level?).

---

## 0. Verdict up front

The architecture is **substantially right**, and better than most research-grant architectures I've seen — subsystem 05 in particular is unusually honest. Your instinct that "this is not a big-data problem" is correct, and it survives scrutiny.

But four things are wrong or unowned, and one of them is load-bearing:

| # | Finding | Severity |
|---|---|---|
| **F1** | **There is no relevance gate on the news pipeline.** Every article — cricket, politics, everything — gets NER'd, RE'd, geo-linked, embedded, and becomes a `:Document` node in the KG. ~95–98% of that work is waste, and it silently breaks the sizing of *three* stores (GPU hours, pgvector, Neo4j). | **High** |
| **F2** | **The "<100 GB lifetime" premise is simply false**, and it is the sentence the entire architecture rests on. Real figure is single-digit TB. The *conclusion* (not big data) survives; the number does not. | **High** |
| **F3** | **No OCR path anywhere.** All PDF extraction assumes digital text. If the historical NIH bulletins are scans, the entire IDSR strategy collapses and the backfill goes from a 2-day job to a 2-month job. | **High — unknown** |
| **F4** | **Historical district-boundary versioning is raised as an open question three times (OQ 02.3, 03.5, 04.1) and owned by nobody.** It silently corrupts the disease panel, which is the product. | **High** |

Plus: the mechanistic prior ontology that every alert's "why" depends on has **no author and no workstream**, and there's a real (small) bug in the connector runner.

Your specific questions are answered in §1–§4. Things you didn't ask about are in §5. Questions I need you to answer are in §6.

---

## 1. Do we need a big-data ingestion pipeline for the historical data?

**No. And the reason is more interesting than "it's small."**

### 1.1 The sizing math, done honestly

| Backfill | Volume | Compute | Verdict |
|---|---|---|---|
| **NIH health (your 500+ files)** | 500 PDFs × ~3 MB ≈ **1.5 GB**. Yields ~500 bulletins × 160 districts × 8 diseases ≈ **640k rows**. | pdfplumber+camelot at 30–120 s/PDF → 4–17 h single-threaded; **~35 min on 16 cores** with `multiprocessing`. | Laptop afternoon. |
| **Weather (15 yr, 160 districts)** | ~876k district-days → 125k district-epiweek rows. A few hundred MB of API JSON. | ~160–500 HTTP calls to Open-Meteo. | Minutes. Bounded by politeness, not compute. |
| **NDMA/PDMA sitreps** | Same order as IDSR. | Same. | Trivial. |
| **News** | **This is the only large one.** See below. | See below. | Large — but *not* in the way you'd expect. |

Health, weather, and disaster backfills are **rounding errors**. A `for` loop with a process pool beats Spark on wall-clock *and* on your time, because you skip the JVM.

### 1.2 The news backfill is an *acquisition* problem, not a compute problem

This is the part nobody has costed. At 2–5k articles/day across 6 outlets, one year of news is ~1M articles.

- **Compute is fine.** Full XLM-R NER + RE + causal + embeddings over 1M docs ≈ **10–30 GPU-hours** on one 4090. A 5-year backfill ≈ 2–6 GPU-days. Annoying, not blocking. Spark does not make a GPU faster.
- **Acquisition is the wall.** Your own connector config (`02 §6.2`) sets `requests_per_minute: 20`. At 20 req/min, **1M articles takes 35 days of continuous polite scraping.** 5 years takes half a year. Dawn/Tribune/Jang publish **no bulk archive**.

So the historical news pipeline is gated by rate limits and archive availability — a problem Spark, Kafka, and every other piece of big-data machinery is completely irrelevant to.

**Three ways out, in order of preference:**

1. **Scope news history to match disease history.** Your IDSR record starts ~2021 (`02 §2.1`). News before your first disease observation **cannot be used for supervised modelling or for the media-lead experiment** — there is nothing to align it to. Scope the news backfill to 2021→present and you halve the problem for free. *This is the cheapest fix and I'd do it regardless.*
2. **Filter on metadata before fetching bodies.** `discover()` gives you title + URL for free from RSS/sitemap/CDX. Apply the cheap keyword/embedding relevance filter (`03 §3.1` — only 2–5% of the firehose is health/climate relevant) **at discovery, and fetch only the bodies that pass.** This cuts fetch volume ~20–50×, which turns 35 days into 1–2 days of polite scraping.
   - ⚠️ **Do not naively throw the rest away.** `05 §2.4` normalizes media surge by *district total news volume*. If you never see the irrelevant articles, you lose the denominator. **Fix: record title+URL+date metadata for every discovered article (cheap, no fetch), fetch bodies only for the relevant ones + a small random control sample.** Denominator preserved, 95% of the fetch cost gone.
3. **Common Crawl / Wayback CDX** for deep history if you truly need pre-2021. Filter the CC URL index by domain, then ranged-fetch only matching WARC records. This is the one place a distributed batch engine could genuinely pay — and even so, it's a one-off.

### 1.3 The thing that actually matters about the backfill

**Your 500+ historical bulletins are not just training data — they are the vintage record, and the entire forecasting evaluation protocol depends on them.**

`05 §2.5` correctly identifies vintage-correctness (as-of joins) as make-or-break: IDSR counts are revised for weeks after first report, and training on revised values while pretending you had them at decision time is how climate-health forecasting papers fool themselves. `05 §8 OQ-3` then asks, worriedly, *"do NIH archives preserve provisional values, or only latest-revised?"*

**Answer: if each weekly bulletin prints prior weeks' numbers alongside the current week, then the bulletin published in week W *is* the snapshot of what was known at W — and your archive of 500 PDFs is a complete vintage history.** You reconstruct `knowledge_time` from the bulletin's own publication week.

This has two consequences the docs miss:

- The IDSR parser **must extract the full retrospective table from every bulletin**, not just the current week's row. If you only parse "this week's numbers," you throw away the vintage record and OQ-3 becomes unanswerable forever.
- **The backfill is therefore the single highest-value data-engineering task in the project** — not because it's big, but because it is simultaneously (a) your only training data, (b) your only vintage record, and (c) the best possible integration test of every downstream stage.

### 1.4 Recommendation

**Do not build a separate big-data pipeline. Build the *backfill execution mode* of the real pipeline, and build it first.**

The docs already have every mechanism needed: Dagster partitioned backfills (`02 §3.2`), `replay_from_bronze` (`02 §4`), and ADR-002 already permits Spark for backfills. What's missing is the *sequencing insight*: the live pipeline is trivially small (a few hundred messages/day on 3 of 4 sources). If backfill works, live works. If you build live first, you validate nothing.

| Route | Pros | Cons |
|---|---|---|
| **Backfill-first, same pipeline (recommended)** | Exercises every stage over a large, varied, drift-heavy corpus; produces the training data and the vintage record; live is then a trivially small special case | Slower to a "look, data is flowing" demo |
| Live-first, backfill later | Fast demo | Validates nothing (live volume is ~200 msgs/day); you discover the PDF layout hell of 2021–2023 late |
| Separate batch pipeline for history | "Simpler" each side | Two codebases, two parsers, two sets of bugs, and the history is parsed by code that never runs again — guaranteed divergence |

---

## 2. Per-source delivery mechanisms — and the "Kafka producer per source" question

### 2.1 First, a reframe: you cannot put a producer *at* a source you don't own

NIH, PMD, NDMA, and Dawn will not run your code. None of them push. **Every source is inherently a pull.** So "a Kafka producer per source" and "the current architecture" are not competing options — the current architecture *is* a Kafka producer per source (ADR-002: *"Kafka carries ALL sources — per-source topics"*). The producer just lives inside a Dagster-scheduled connector on your infrastructure, which is the only place it can live.

The proposal deliverable — *"Implemented Kafka producers and topic schemas for NIH, PMD, NDMA/PDMA and news data sources"* — is therefore met literally.

### 2.2 The actual delivery mechanism, per source

| Source | What the source actually is | Delivery mechanism | Trigger | Why nothing else is possible |
|---|---|---|---|---|
| **NIH IDSR** | Weekly PDF on a WordPress site. URLs **not templatable** (`Weekly_Report-39-2025.pdf` vs `IDSR-Weekly-Report-16-2022.pdf`), month folder varies, `?v=NN` cache-busters | **Scrape the listing page → HTTP GET the PDF** → bronze → tiered table extraction → Kafka | Dagster cron, weekly (Tue 06:00 PKT) + zero-discovery guard | No API, no feed, no push. Discovery *must* scrape; you cannot construct the URL. |
| **PMD / CDPC** | Paid, **single-end-user licence**, no open API, licence forbids onward dissemination | **Not used in Phase 1.** Primary = **Open-Meteo Archive REST API** (ERA5/ERA5-Land, CC-BY 4.0), batched GET per district centroid | Dagster cron daily; `district × week` multipartition | The PMD licence is incompatible with a redistributable research platform (OQ 02.1). Reanalysis is the only legally clean district-level history. |
| **NDMA / PDMA** | Daily SITREP PDFs with **opaque random filenames** (`KRgdunvv4Nksroa49bU7.pdf`) | **Scrape listing page + Dagster sensor** on new entries; **ReliefWeb JSON API** in parallel as structured redundancy | Daily in monsoon, 2–3×/wk off-season, + sensor | Filenames are unguessable. ReliefWeb is the *only* real API in the disaster domain — use it as a cross-check, not a replacement. |
| **News** | RSS/Atom feeds + HTML article bodies | **RSS/sitemap poll → fetch article HTML** → trafilatura | Dagster schedule, 15–30 min | RSS *is* the closest thing to a push feed that exists. No Pakistani outlet offers webhooks. |

Every one is a poll. What differs is *what you poll* (listing page / RSS / REST), *how often*, and *what comes back* (PDF bytes / JSON / HTML).

### 2.3 Is Kafka overkill for the low-frequency sources? Honestly: yes, on throughput grounds

Let's not pretend otherwise.

- IDSR: **1 document/week** (~1,300 records).
- PMD: **160 records/day**.
- NDMA: **a few documents/day**.
- Total non-news: **~200–1,500 messages/day.**

Kafka is engineered for millions of messages per second. Running a broker + schema registry + consumer groups + DLQ topics + offset management for 200 messages a day is, in pure engineering terms, **not justified by throughput**. ADR-002's rationale is honest about this — it says the alternative "contradicts the deliverables list," which is a *contractual* argument, not an engineering one. Good. That's the truth and the doc should keep saying it.

**But it's still the right call, for three reasons that are not throughput:**

1. **Fan-out.** The same source record is consumed by the normalizer, the KG builder, the media aggregator, and (later) whatever a student invents. Kafka makes that N-consumer fan-out free and decoupled. Point-to-point writes into Postgres would push that coupling into application code.
2. **Uniformity, which is the real risk you're managing.** Your dominant risk (stated in `00 §1`, and I agree) is **team turnover and operational entropy**, not throughput. Two ingestion patterns = two wire contracts, two dedup strategies, two replay stories, two sets of bugs, and a student who learns the wrong one. One pattern for all four sources is worth real money against that risk.
3. **It's a funded deliverable**, and it's cheap: a single-broker KRaft Kafka in Compose is 4 GB RAM / 2 vCPU (`07` sizing). This is a service, not a cluster.

**Verdict: keep Kafka everywhere. But state the justification honestly (fan-out + uniformity + deliverable), and stop implying the throughput case exists.** Self-deception about why you chose X is how teams end up over-building Y.

### 2.4 The one thing I'd change: make the consumers *batch drains*, not long-running streams

**This is a genuine gap — the docs never say which it is,** and it matters a lot operationally.

For IDSR/PMD/NDMA, a long-running Kafka consumer process idles 99.99% of the time. It can die silently, rebalance, lag, and — worst — **Dagster's asset graph becomes a lie**, because Dagster thinks it orchestrates the pipeline while Kafka offsets actually determine what got processed. Two orchestrators, one pipeline, no single authority on "was week 39 processed?"

**Recommendation: for the three low-frequency sources, the normalizer is a Dagster asset that performs a bounded drain** — on materialization, read from the last committed offset to the current high-water mark, process, upsert, commit offsets, done.

| | Long-running consumer | **Batch drain (recommended)** |
|---|---|---|
| Who owns scheduling | Kafka (implicitly) | **Dagster** — one orchestrator |
| Lineage / "did week 39 run?" | Opaque (check consumer lag) | **Asset materialization = the answer** |
| Failure mode | Silent death, lag creep | Failed asset run, visible + retryable in the UI |
| Ops cost for weekly data | A process babysat 24/7 to do 1 thing/week | Nothing running between weeks |
| Kafka still in the path? | Yes | **Yes** — deliverable is real, not a tee |

News is the exception: it's a genuine stream, and the Spark job owns its own checkpoint. That's correct and should stay.

### 2.5 A note on Spark, since it's the same argument

`07`'s sizing table says Spark runs as a **"local-mode job container, on demand… not resident."** That is worth saying out loud: **Spark here is a library with a JVM, not a cluster.** ADR-002 justifies it as "real stream, real parallel inference," which at 0.06 articles/second and one GPU is not true. But `03 §6.3` makes the *right* call anyway — models live in an external Triton/vLLM inference service, **not** in Spark executors — which means Spark is a thin orchestration layer over gRPC calls, and its cost is near zero.

The honest justification for Spark is: **(a) it's a committed deliverable, (b) `03 §6.5` reuses the identical code path for the historical backfill and the live stream** — which is literally what the proposal promises ("*unified streaming–batch processing design… consistent analytics logic for both real-time situational monitoring and retrospective model training*"), and **(c) Structured Streaming's stateful dedup over a 7-day watermark is a real thing you'd otherwise hand-roll.** That's a defensible case. The "parallel inference" claim is not, and should be removed from ADR-002.

---

## 3. Why is Kafka *after* the bronze layer? Doesn't Kafka come first?

**Your instinct describes the textbook pattern. The architecture deliberately inverts it, and it is right to.**

### 3.1 First: the diagram is misleading, and that's why the question arises

`00-ARCHITECTURE-OVERVIEW.md` draws:

```
raw archive ──────────────► MinIO (bronze)
       │
       ▼
┌─────────  KAFKA BACKBONE  ─────────┐
```

which reads as *"MinIO feeds Kafka."* **It does not.** The runner in `02 §1.3` is unambiguous:

```python
object_key = ctx.bronze.put(conn.name, raw)     # 1. raw bytes → MinIO
...
for rec in conn.parse(raw, ctx):
    ctx.producer.produce(conn.kafka_topic, ...)  # 2. parsed records → Kafka
```

**One connector process writes to both.** Raw *bytes* go to MinIO; parsed, validated, source-native *records* go to Kafka. Kafka never reads MinIO. **The overview diagram should be redrawn** — it is actively causing this confusion.

### 3.2 The two canonical patterns

**Pattern A — Kafka-first (the one you're thinking of).** The log is the front door; the lake is a projection of the log.
```
source → producer(raw bytes) → Kafka raw topic → sink connector → object store
                                     └→ stream processors
```

**Pattern B — Archive-first (what CHIP does).** The object store is the system of record; Kafka carries derived records.
```
source → connector → object store (immutable raw bytes)
                          └→ parse → validate → Kafka (structured records) → consumers
```

### 3.3 Why B is correct here — four reasons, the first being decisive

1. **Your payloads are blobs, not events.** IDSR bulletins and NDMA sitreps are 1–5 MB PDFs. Kafka's default `max.message.bytes` is **1 MB**; large messages degrade broker heap, replication, and consumer fetch. To do Pattern A you would have to either raise the message limit and eat the broker cost, or apply the **claim-check pattern** — write the blob to object storage and put a *pointer* on the bus.
   **But the claim-check pattern *is* Pattern B.** Even a "Kafka-first" design converges on writing the blob to MinIO first. CHIP is doing claim-check correctly. This is the strongest argument and the docs never make it.

2. **Retention asymmetry.** Bronze is permanent (`02 §1.7`: *"never auto-delete bronze"*). Kafka retention is finite (`02 OQ-8` proposes 90 days). Make Kafka the system of record and you need infinite retention — which makes Kafka an archive, and Kafka is a *bad* archive: no random access by key without scanning a partition, no browsing, no HDD tiering. Compare with content-addressed MinIO: `GET bronze/nih_idsr/2025/W39/sha256-ab12.../report.pdf` is O(1), browsable, and lives on cheap spinning disk.

3. **Archive-before-parse is a durability guarantee, and parsing *will* fail.** The entire `02 §5` quarantine flow exists because NIH will change its bulletin layout. When parse fails, you must still have the bytes. Pattern B guarantees this structurally. Pattern A-with-parsed-records loses the bytes on a parse failure at ingress; Pattern A-with-raw-bytes is back to problem (1).

4. **Two replay tiers, serving different failure modes** (`02 §4`): re-parse from bronze with a bumped `transform_version` (for parser improvements — works forever, on any historical document), *and* re-consume from Kafka with a new consumer group (for downstream logic changes — fast, but only within retention). Pattern A gives you only the second, and only within retention. You could never re-parse a 2022 bulletin with a better table adapter.

### 3.4 The honest cost of Pattern B, which the docs do *not* acknowledge

**The connector is a dual writer** — MinIO then Kafka, two systems, no shared transaction. The dedup layers in `02 §1.4` handle most of it correctly (content-hash dedup is recorded *after* the produce loop, so a mid-loop crash re-produces everything, and idempotent upserts at the normalizer make that safe — this is at-least-once done right).

**But there is a real bug:** `confluent-kafka`'s `produce()` is **asynchronous** — it buffers. The runner calls `ctx.dedup.record_content(...)` and `ctx.watermark.advance(...)` with **no `producer.flush()`**. A crash between the last `produce()` and the buffer flush loses those messages *while having marked the content as produced* — so the next run skips them. Silent data loss.

**Fix:** flush (and check delivery reports) before `record_content` / `watermark.advance`.

```python
for rec in conn.parse(raw, ctx):
    ...
    ctx.producer.produce(conn.kafka_topic, envelope(rec, prov))

ctx.producer.flush()                       # ← MISSING. must succeed before we claim success
ctx.dedup.record_content(conn.name, raw.content_hash)
ctx.watermark.advance(conn.name, item)
```

---

## 4. "Prove to me that the analytical core, the modelling, and the KG work"

### 4.1 Analytical core — it works, *if and only if* you add a relevance gate (F1)

**The core operation of this entire platform is one join:** disease counts × weather × hazard flags × media signals, on `(district, epi-week)`. That is a star-schema join on a conformed grain, and it is why Postgres-with-extensions is exactly right:

| Table | Rows (15 yr, 160 districts) |
|---|---|
| Disease panel (5 diseases × 800 weeks) | ~640k |
| Climate panel (10 vars × 800 weeks) | ~1.3M |
| Media signals | ~100k |

**A few million rows.** Postgres answers this in milliseconds with an index. Timescale is arguably *unnecessary* at this size — a plain partitioned table would do — but it's free, gives you `time_bucket` and continuous aggregates, and costs nothing. Keep it. Cross-domain joins staying in one database (rather than being stitched in application code across five engines) is worth more than any per-engine optimization at this scale. **ADR-003 is correct.**

**Now the problem.** `03 §6.1` runs *every* ingested article through NER → RE → causal → geo-link → **embed**, and `04 §2.1` turns every one into a `:Document` node with `:Evidence` spans. `03 §3.1` states plainly that **only 2–5% of the news firehose is health/climate relevant.** Nobody connected these two facts.

At 1M articles/year, with no relevance gate:

| Store | With no gate | With a gate (2–5% pass) |
|---|---|---|
| **GPU** (full enrichment) | 10–30 GPU-hours/yr of corpus; a 5-yr backfill = **2–6 GPU-days** | **~1–2 GPU-hours.** 20–50× cheaper |
| **pgvector** | ~3M chunks × 1024-dim × 4 B ≈ **12 GB of vectors**, HNSW index needs ~15–20 GB RAM. Postgres is allocated **16 GB total** (`07`) with `shared_buffers=4 GB`. **This will not build.** | ~150k chunks ≈ **600 MB.** Comfortable. |
| **Neo4j** | 1M `:Document` + several million `:Evidence` per year ⇒ **5–10M nodes**, tens of millions of edges. `04 §3.3` sizes for *"tens of thousands of nodes, ≤ low-millions edges"* on 8 GB pagecache. **Off by an order of magnitude.** | 20–50k documents/yr ⇒ **~2–3M nodes total.** Exactly the stated design point. |

**One change fixes all three.** Add a **relevance gate as a new cheap stage in `03 §6.1`, between dedup [2] and NER [3]**: keyword/lexicon filter, then an embedding-similarity check against seed paragraphs. Cost: milliseconds/doc on CPU. Everything downstream — GPU, pgvector, Neo4j — shrinks 20–50×.

**Caveats to respect when you build it:**
- **Keep counting what you drop.** `05 §2.4` normalizes media surge by *district total news volume*. Count every article (title/lang/geo is cheap); only *enrich* the relevant ones. The denominator must survive the gate.
- **Log the gate's decisions and sample them.** A relevance gate is a recall risk — an outbreak reported in an unusual framing could be filtered out. Sample 1% of rejects into the review queue and measure the false-negative rate. This makes the gate a *measurable* component, not a silent filter.

> **This single finding — F1 — is the most consequential thing in this document.** Without the gate, `00`'s "not a big-data problem" premise quietly becomes false, and someone will "fix" it by adding a Spark cluster.

### 4.2 F2 — while we're here, the headline number is wrong

`00 §1` says *"under 100 GB over the project's life."* `07 §1` says *"tens of GB/year"* and sizes MinIO at 2–4 TB. These do not agree with each other, and neither agrees with reality:

- News bronze at 2–5k articles/day × 50–150 KB raw HTML = **40–200 GB/year uncompressed**, ~20–30 GB/yr gzipped. Over 4 years, **80–120 GB of news bronze alone.**
- Plus PDFs, Postgres, Neo4j, pgvector, MLflow artifacts, Kafka retention, backups.

**Real figure: single-digit TB lifetime.** The *conclusion* — "this is a hard-integration problem, not a big-data problem" — **still holds** (a few TB on one box with 2×8 TB HDDs is not big data). But fix the number, because the false number is load-bearing rhetoric and the moment someone catches it, they'll challenge the conclusion too.

### 4.3 Modelling — this is the strongest document in the set, and here's why it will work

Subsystem 05 gets right the things that almost every climate–health forecasting effort gets wrong. Specifically:

1. **Vintage-correct as-of joins (§2.5).** Training on revised counts while pretending you had them at decision time is *the* standard way this literature fools itself. They designed against it from row zero. This alone puts the project ahead of most published work.
2. **Proper scoring rules (CRPS, log score) on predictive distributions**, not MAE on point forecasts. Disease counts are small, non-negative, and over-dispersed; NB likelihood + CRPS is the correct treatment.
3. **NB-GLM + DLNM cross-basis as the *primary product*, not the LSTM.** DLNM (Gasparrini) is the field standard for non-linear × lagged exposure–response, it produces the relative-risk curves that are themselves a committed deliverable, and epidemiologist reviewers will recognize it instantly.
4. **The honest data-sufficiency statement (§1.4):** ~160 districts × a few hundred weeks = 200–400 points per district. They say plainly that per-district LSTMs are not viable, that global models are the only option, and that **they expect deep models to lose to a well-specified GLM.** That is correct, and it is rare to see it written down before the experiment.
5. **The media-lead hypothesis is made falsifiable (§1.5), with an explicit echo guard.** The project's headline claim — *news precedes official surveillance* — is tested (does media add skill *before* counts rise?) rather than assumed. If it turns out news mostly *echoes* outbreaks, that's a publishable negative result and the architecture survives it.

**Does the math actually support it?** Yes, with one caveat. If IDSR history starts ~2021, you have ~250 weekly points per district-disease and ~5 dengue seasons. For a *pooled* NB-GLM across 160 districts that's ~40,000 observations for one disease against a cross-basis of ~15–25 df. **That's comfortable.** A DLNM will fit. An LSTM will not beat it.

**The binding constraint on this entire project is neither compute nor throughput — it is the depth of the disease record.** Every additional year of IDSR bulletins you can scrape is roughly a 20% increase in your training data. Which loops straight back to §1.3: **the historical PDF backfill is the highest-leverage engineering task you have.**

**Where I'd push back on 05:** `§8 OQ-8` notes 160 districts × 5 diseases = **~800 alert candidates per week** and asks whether there's review capacity. There isn't — no human reviews 800 candidates weekly. This needs a triage design (severity pre-filter + district risk-tiering), and the doc's own worry ("does pre-filtering risk hiding true early signals?") is the right one. This should become a designed component, not an open question.

### 4.4 The Knowledge Graph — the weakest justification in the architecture, and how to prove it

I'll be blunt, because you asked me to prove it rather than assert it.

**The KG's design is excellent. Its *justification* is the thinnest thing in the stack.** Ask the question that matters:

> **What can the CHKG answer that a SQL join on the gold panel cannot?**

Run the honest audit:

| Question a stakeholder actually asks | Needs a graph? |
|---|---|
| "Dengue cases in Lahore, W27, with the preceding 3 weeks of rainfall" | **No.** SQL. |
| "Why was dengue flagged in Lahore?" — feature attributions, climate anomaly, model version, data vintage | **No.** That's `05 §5.7`'s evidence bundle — a JSONB column. |
| "Which news articles support the flood→cholera claim in Larkana?" | **No.** A join to `documents` + pgvector. |
| "Traverse: HeavyRainfall → StandingWater → AedesBreeding → Dengue, with supporting evidence at each hop" | **Yes.** Variable-length multi-hop over a mechanistic chain. Recursive CTEs can do it; Cypher does it *far* better. |
| "Find claims structurally similar to this one in districts we haven't flagged" | **Yes.** Graph-shaped. |

**So the honest case for Neo4j is exactly two things:**
1. **Multi-hop explainability traversal** over curated mechanistic priors + observed facts + extracted claims.
2. **The GraphRAG retrieval skeleton** (`04 §6.1`): the subgraph supplies the *reasoning structure with epistemic status*, pgvector supplies the *prose evidence*. That combination is what makes the summaries auditable rather than plausible.

**It is *not* needed for the analytics** (SQL) **and it is *not* needed for the GNN** — `04 §5.3` exports Neo4j → pandas → PyG, and you could go Postgres → pandas → PyG and skip the graph entirely for the ML path.

That's fine! (1) and (2) *are* the funded deliverables — "explainable, auditable, evidence-grounded summaries for policy stakeholders." But be clear-eyed: **if the KG ends up a thin wrapper over the panel plus a handful of priors, the "why" queries will be shallow and the RAG summaries will be no better than templating the alert's evidence bundle.**

#### 4.4.1 The gap that decides whether the KG is real: nobody is authoring the priors

`04 §1.2` defines `derivation: 'domain_prior'` and `§4.4`'s governance query **explicitly exempts `domain_prior` assertions from needing evidence.** Meanwhile `05 §5.7` promises that every issued alert carries a KG path like `HeavyRainfall → StandingWater → AedesBreeding → Dengue`.

**Those mechanistic intermediate nodes are not in any data source.** They don't come from NIH, PMD, NDMA, or the news. They are hand-curated domain knowledge — and **there is no workstream, no owner, and no deliverable anywhere in the seven subsystem docs that creates them.**

Without them, the multi-hop traversal in 4.4's table — the *only* thing that justifies Neo4j — has nothing to traverse.

**Fix:** a **curated mechanistic prior ontology** as a Phase-0/1 deliverable. ~50–200 assertions, each `TRIGGERS`/`ASSOCIATED_WITH`, each `status:'hypothesized'`, **each citing a peer-reviewed paper as its `Evidence`** (which also removes the ugly `domain_prior` exemption from the governance query — every claim gets evidence, no exceptions). This is bounded, defensible, thesis-able, and it is the difference between a KG that reasons and a KG that decorates.

#### 4.4.2 The one-week experiment that proves or kills the KG

You asked me to prove it works. **I can't prove it from the documents, and neither can anyone else — so make it falsifiable before you build it:**

> **Write down 10 questions a real NIH/NDMA analyst would ask. For each, write both the Cypher and the SQL.**
> - If **≥7 are strictly easier or only possible in Cypher** → the graph earns Neo4j, build it as designed.
> - If **most are SQL-shaped** → cut Neo4j's scope to the RAG evidence layer only (documents, evidence spans, assertions) and keep the facts in Postgres, where they already live.

Cost: one week, one student, zero infrastructure. It converts the largest unjustified component in the architecture into a decision backed by evidence. **Do this before Phase 3.**

---

## 5. Issues you didn't ask about

### 5.1 F3 — There is no OCR path, anywhere. This is the biggest unknown in the project.

The entire IDSR extraction strategy (`02 §2.1`) is **pdfplumber → camelot → LLM-fallback**, and the same tiered approach is reused for NDMA sitreps. **All three tiers assume the PDF contains a digital text layer.**

Pakistani government PDFs — especially older ones — are frequently **scanned images**. On a scanned page:
- `pdfplumber` returns **nothing**.
- `camelot` (both lattice and stream) **fails outright** — it needs a text layer.
- The "LLM fallback" as specified sends *text* to an LLM. There is no text. It would need to be a **vision** model, which is a different model, different VRAM budget, and different (much worse) error characteristics for numeric table cells.

**The word "OCR" does not appear in any of the seven subsystem documents.**

If even 30% of your 500+ historical bulletins are scans, the backfill needs a full OCR stage (Tesseract / PaddleOCR / docTR / Surya), plus table-structure recovery, plus a validation regime tight enough to trust OCR'd *case counts* — and OCR digit errors on epidemiological counts are exactly the kind of silent corruption that destroys institutional trust.

**This single fact determines whether the historical backfill is a 2-day job or a 2-month job.** It is question #1 in §6.

### 5.2 F4 — District boundary versioning is raised three times and owned by nobody

`02 OQ-3`, `03 OQ-5`, and `04 OQ-1` all independently ask the same question: **Pakistan created new districts between 2021 and 2025. How do we handle it?** None assigns an owner, and no subsystem designs for it.

This is not a documentation nit — **it silently corrupts the panel, which is the product.** If a tehsil split out of Sialkot in 2023, then "Sialkot" in a 2022 bulletin and "Sialkot" in a 2025 bulletin are **different geographies**, and your 5-year dengue series for Sialkot has an undeclared structural break that the DLNM will happily absorb as a real climate effect.

**Recommended design (standard practice in spatial epidemiology):**
- Gazetteer rows carry **`valid_from` / `valid_to`** — it is a slowly-changing dimension, not a static lookup.
- The panel stores the P-code **as reported at the time**, preserving fidelity to the source.
- A separate, versioned **crosswalk** maps historical P-codes onto a single declared **"analysis vintage"** (e.g. 2025 boundaries), with an explicit rule for splits (proportional allocation by population) and merges (sum).
- Every analytical query declares which vintage it's using. Model cards record it.

This must be a **Phase-0 blocking decision**, not an open question — the gazetteer is a Phase-0 deliverable and everything joins through it.

### 5.3 Duplicate near-dup detection, in two subsystems, with no owner

- `02 §2.4` (news connector): *"near-dup clustering via SimHash Hamming distance… store the cluster id."*
- `03 §6.1` stage [2] (Spark): *"DEDUP (content hash + MinHash/LSH near-dup over 7-day state)."*

**Two implementations of the same thing.** Worse, `03 OQ-8` asks *"do we need cross-outlet story clustering?"* — a question `02` believes it already answered. The two docs are unaware of each other. The reconciliation pass that produced ADRs 006–011 missed this seam.

**Recommended split (clean and defensible):**
- **Connector**: exact-URL identity + content-hash dedup. Cheap, local, per-outlet. It's all a single-outlet connector *can* do.
- **Spark**: cross-outlet near-dup clustering. Wire copy (the same PPI/APP story in Dawn *and* Tribune) requires a **global view across outlets**, which no per-outlet connector has. This is genuinely Spark-shaped (stateful, watermarked) and is the best argument for Structured Streaming in the whole design.

Left unfixed, wire-copy reprints inflate `media_mentions` — which is a **direct bias in the media-surge feature**, i.e. in the project's headline hypothesis.

### 5.4 Unreconciled ADR leftovers (documentation, but it matters here)

The reconciliation pass annotated *some* contradiction points and missed others. In a rotating-student project **the docs are the system**, and a student implementing from `03` today will build the wrong thing:

| Doc | Says | ADR says | Status |
|---|---|---|---|
| `03 §1.3`, `§2.4`, App. B | `multilingual-e5-base` (**768-dim**) | **ADR-011**: BGE-M3, **1024-dim** | ❌ Not annotated. A dimension mismatch = full table rebuild + re-embed. |
| `03 §2.4` JSON, App. A | `gadm_id`, `district_gadm` | **ADR-006**: COD-AB `pcode` | ⚠️ Annotated in §5.1 only; the schema and worked example still show GADM. |
| `07` sizing table | **Karapace** | **ADR-009**: **Apicurio** | ❌ Not annotated. Different backup story. |

### 5.5 The GPU budget doesn't add up

| Doc | Assumes |
|---|---|
| `07` sizing | LLM serving = **7–8B** model, 4-bit, "~10–14 GB VRAM, fits alongside an embedding model on one 24 GB card" |
| `03 §1.3` | **Qwen3-30B-A3B Q4 (~17 GB)** |
| `04 §6.2` | **Qwen3-14B / 32B** (~17–20 GB at Q4) |

On **one 24 GB card** you cannot simultaneously host: XLM-R NER + XLM-R RE + BGE-M3 embedder (Triton, resident) **and** a 30–32B LLM at Q4 (~17–20 GB, vLLM) **and** serve interactive RAG to a dashboard during working hours.

`03 OQ-9` flags "GPU contention policy" as an open question, but `07`'s hardware plan doesn't resolve it — and it determines **whether the RAG demo is even possible while the pipeline is running.** This needs to be a *decision*:
- **Buy the second 24 GB card** (`07` already lists 2× used RTX 3090 as an option — this is the cheap answer), **or**
- **Explicitly time-slice**: encoders (small, ~4 GB) resident on the GPU for online serving; the LLM batch (causal fallback, distillation) runs nightly; interactive RAG loads a *smaller* model (7–8B, per `07`) during the day.

Pick one and write it down. Right now, three documents assume three different machines.

---

## 6. Questions I need answered before any of this is finalized

These are the ambiguities I refuse to assume my way through. Ordered by how much they change the plan.

1. **Are the 500+ historical health PDFs digital-text, or scanned images?** *(§5.1 — the biggest fork in the project.)* If scans: we need an OCR stage, a vision model for the fallback, and a much tighter numeric-validation regime — and the backfill estimate goes from days to weeks. Please open five of them, from different years, and try selecting text.
   **And:** are they all IDSR weekly bulletins, or a mix (line lists, Excel, DHIS2 exports, DEWS bulletins)? "500+ files" could mean five different parsing problems.

2. **Does each weekly IDSR bulletin print *prior weeks'* numbers alongside the current week?** *(§1.3.)* If yes → your archive is a complete vintage record, `05`'s entire backtest protocol is saveable, and **the parser must be built to extract the full retrospective table.** If no → vintage-correct backtesting is impossible, and every forecast-skill claim in the project needs a loud caveat. This changes the parser spec.

3. **How far back does the news corpus need to go, and how do you intend to acquire it?** *(§1.2.)* Polite scraping physically cannot fetch a decade of Dawn. Options: scope news to match the disease record (~2021+), metadata-first + relevance-gated fetch, or Common Crawl/Wayback. I recommend all three, but the depth requirement is your call and it changes the ingestion plan.

4. **What are the "other sources" whose volume you expect to be comparable to the 500+ health files?** You said none have been collected yet. Which ones, and what shape (PDF / CSV / API / HTML)? The sizing in §4.1–4.2 assumes news dominates; if there's an unlisted large source, that changes.

5. **What is your actual target news volume — how many articles/day across how many outlets?** *(Drives F1 and F2.)* `02 §2.4` lists six outlets; "low-thousands/day" is used everywhere but never sourced. This one number determines the GPU, pgvector, and Neo4j sizing.

6. **One GPU or two?** *(§5.5.)* Three documents assume three different machines. This decides whether interactive RAG can coexist with the pipeline.

7. **Who owns the mechanistic prior ontology, and is an epidemiologist available to author/validate it?** *(§4.4.1.)* Without it, the KG has nothing to traverse and the "why" queries are empty. The proposal's stated weakness — *"limited in-house domain expertise in epidemiology"* — lands squarely on this deliverable.

---

## 7. Summary of recommended changes

| # | Change | Effort | Impact |
|---|---|---|---|
| **R1** | **Add a relevance gate** to the news pipeline (before NER), keeping a full metadata count for the media-surge denominator | Small | **Huge** — 20–50× reduction in GPU, pgvector, and Neo4j; makes the stated sizing true |
| **R2** | **Correct the "<100 GB" premise** in `00 §1` to a real figure (single-digit TB); keep the conclusion | Trivial | Protects the whole "not big data" argument from being discredited |
| **R3** | **Decide the OCR question** (needs data, see Q1) and add an OCR tier to the PDF extractor if required | Unknown | Blocks the backfill |
| **R4** | **Design district-boundary versioning** as a Phase-0 gazetteer feature (valid_from/valid_to + crosswalk + analysis vintage) | Medium | Prevents silent corruption of the panel |
| **R5** | **Make low-frequency Kafka consumers Dagster-orchestrated batch drains**, not long-running consumers | Small | One orchestrator, honest lineage |
| **R6** | **Fix the missing `producer.flush()`** in the connector runner | Trivial | Prevents silent data loss |
| **R7** | **Split near-dup ownership**: exact/content-hash in the connector, cross-outlet clustering in Spark | Small | Removes duplicate work; de-biases the headline media feature |
| **R8** | **Run the 10-question Cypher-vs-SQL test** before building the CHKG | 1 week | Converts the least-justified component into an evidence-backed decision |
| **R9** | **Fund a curated mechanistic prior ontology** (~50–200 cited assertions) as a Phase-1 deliverable | Medium | Without it, the KG cannot answer "why" |
| **R10** | **Resolve the GPU budget**; **redraw the `00` diagram** (bronze→Kafka arrow is wrong); **finish the ADR reconciliation** (e5→BGE-M3, Karapace→Apicurio, GADM→P-code in `03`) | Small | Removes three ways for a new student to build the wrong thing |
| **R11** | **Remove the "real parallel inference" claim** from ADR-002; replace with the honest justification (deliverable + unified batch/stream code + stateful dedup) | Trivial | Intellectual hygiene — the reason you keep a tool determines where else you'll misuse it |

---

## 8. Disposition (2026-07-13) — where each recommendation landed

| # | Landed in |
|---|---|
| R1 relevance gate | **ADR-013** (free, via the NAaaS keyword API) + `03 §0.1` (incl. the two obligations: **keep the denominator**, **measure the gate's recall**) |
| R2 storage figure | `00 §1.1`, `07 §0` |
| R3 OCR / parsing | **ADR-012** (agentic parse + immutable bronze cache + `access_tier` gate + **mandatory total-reconciliation**), `02 §2.1`, `02 §2.3` |
| R4 boundary versioning | **ADR-015 Part A** (pins 01 §1.4; population-weighted apportionment; `analysis_vintage` on every mart and model card) |
| R5 batch drains | **ADR-002 §A3**, `02 §1.6` |
| R6 `producer.flush()` | `02 §1.3` |
| R7 near-dup ownership | **ADR-013** (NAaaS owns exact/near-dup; CHIP owns cross-outlet wire clustering **only if NAaaS doesn't return a cluster id** — 02 OQ-5), `03 §6.1` stage [2] |
| R8 Cypher-vs-SQL gate | `04 §0.1` — **one student-week, before Phase 3** |
| R9 mechanistic priors | `04 §0.2`, `ontology/` in the monorepo, Phase 1–2 in the plan, **OQ-C** |
| R10 GPU / diagram / reconciliation | **ADR-014**; `00 §2.1`; e5→BGE-M3 + GADM→pcode in `03`, Karapace→Apicurio in `07` |
| R11 ADR-002 honesty | **ADR-002 §Amendment** (A1 Kafka, A2 Spark, A3 batch drains) |

**New findings that emerged while applying these** (they weren't in the original list and are now tracked):

- **Alert triage was an open question and cannot be** — ~800 candidate slots/week is unreviewable, and review is the *only* thing between a model output and a government stakeholder. Designed in `05 §5.4.1`: **rank don't threshold, defer don't discard, reserve a quota for `media early-signal`** (the weakest, earliest, most-triage-vulnerable class — and the project's headline capability), and **measure the missed-outbreak rate among *unreviewed* candidates**, which is the only number that distinguishes triage from suppression.
- **The `domain_prior` evidence exemption is now unnecessary and has been deleted** (`04 §4.4`). Once every mechanistic prior cites a paper, *every* claim in the CHKG carries evidence — a strictly stronger and simpler invariant than "every claim except these."
- **The evidence-offset-rot problem (04 OQ-10) became an external contract**, not an internal one: the normalized text now lives in NAaaS, so **NAaaS** must guarantee durability (**ADR-013 contract C2**) — or CHIP re-archives full text itself.
- **Two new sources of bias in the headline media feature**, both ingestion contracts rather than modelling problems: NAaaS district coverage may be metro-biased, and **unresolved wire copy inflates `media_mentions` directly** (05 OQ-3).
