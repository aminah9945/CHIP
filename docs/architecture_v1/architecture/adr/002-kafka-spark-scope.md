# ADR-002: Kafka Everywhere, Spark Only Where It Earns Its Keep

- **Status:** **Accepted** (2026-07-10; **amended 2026-07-13** — see §Amendment)
- **Context:** The NRPU proposal commits to "Apache Kafka + Spark Structured Streaming." The deliverables list promises Kafka producers and topic schemas for NIH, PMD, NDMA/PDMA and news sources, and a validated Kafka cluster. The methodology text names Spark specifically for NLP workflows and streaming–batch unification. IDSR is weekly, PMD daily, NDMA event-driven, news near-continuous. Data volume is small; Spark clusters are a permanent maintenance liability for a student team.

## Decision

- **Kafka carries ALL sources** — per-source topics, schema-registry-governed. This fully satisfies the deliverables list and provides the replay/decoupling backbone.
- **Spark is scoped to two jobs:** (1) the news NLP enrichment pipeline, and (2) historical backfills/reprocessing when models or parsers improve. Spark runs in **local mode as an on-demand job container** (07 §1.1) — it is a library with a JVM, **not a cluster**.
- **All low-frequency sources** (IDSR, PMD, NDMA/PDMA) are handled by plain Python producers/consumers orchestrated by **Dagster**.

---

## Amendment (2026-07-13): honest justification, and the consumer-shape rule

### A1. The throughput argument for Kafka does not exist. Say so.

The original text implied an engineering case that isn't there, and a self-serving justification for one tool is how a team ends up misusing the next one. The real numbers:

| Source | Volume |
|---|---|
| NIH IDSR | **1 document/week** (~1,300 records) |
| PMD (Open-Meteo) | ~160 records/day |
| NDMA/PDMA | a few documents/day |
| **Total non-news** | **~200–1,500 messages/day** |

Kafka is engineered for millions of messages per second. It is being run at roughly one hundred-thousandth of capacity. **Kafka is not justified by throughput, and no CHIP document should claim it is.**

**Kafka is justified by three things, all of which are real:**
1. **Fan-out.** One source record is consumed by the normalizer, the KG builder, the media aggregator, and whatever a future student adds. Kafka makes N-consumer decoupling free; direct-to-Postgres writes would push that coupling into application code.
2. **Uniformity against the actual dominant risk.** CHIP's stated top risk is **team turnover and operational entropy**, not load. Two ingestion patterns means two wire contracts, two dedup strategies, two replay stories, and a student who learns the wrong one. One pattern across all sources is worth real money against that risk.
3. **It is a contracted deliverable**, and it is cheap: single-broker KRaft in Compose, 4 GB RAM / 2 vCPU. A service, not a cluster.

### A2. The "real parallel inference" claim for Spark is withdrawn

The original ADR justified Spark on the news stream as *"real stream, real parallel inference."* Both halves are false:

- At low-thousands of articles/day, the news stream is **~0.06 articles/second**. A single Python process handles this with a 100× margin.
- 03 §6.3 correctly puts the models in an **external Triton/vLLM inference service**, *not* in Spark executors. Spark therefore performs **no inference at all** — it is a thin orchestration layer issuing gRPC calls. Spark does not make a GPU faster.
- **ADR-013** (news via the NAaaS API) removes the continuous scrape entirely. News now arrives by scheduled API pull. **There is no genuinely continuous stream anywhere in CHIP.**

**Spark's honest justification, which is sufficient:**
1. **It is a committed deliverable** (the proposal names Spark Structured Streaming explicitly).
2. **Unified batch/stream code** (03 §6.5): the *identical* pipeline code runs the live micro-batch and the one-off historical enrichment over the news backlog — which is literally what the proposal promises ("*consistent analytics logic for both real-time situational monitoring and retrospective model training*"). This is the strongest real argument and the original ADR never made it.
3. **Stateful, watermarked cross-outlet near-dup clustering** is something Structured Streaming provides and a hand-rolled consumer would have to build.
4. **The cost is near zero** — local mode, on demand, not resident.

Keep Spark. Stop overselling it.

### A3. Low-frequency Kafka consumers are Dagster **batch drains**, not long-running streams

The original ADR said low-frequency sources use "plain Python consumers" without specifying their *shape*. This matters, and the answer is not obvious.

**A long-running consumer for weekly data is a liability:** it idles 99.99% of the time, can die silently, can lag or rebalance, and — worst — it makes **Dagster's asset graph a lie**, because Dagster believes it orchestrates the pipeline while Kafka offsets actually decide what got processed. Two orchestrators, one pipeline, no single authority on *"was week 39 processed?"*

**Rule:** for `nih_idsr`, `pmd_weather`, `ndma_sitrep` (and any future low-frequency source), the normalizer is a **Dagster asset that performs a bounded drain**: on materialization, consume from the last committed offset to the current high-water mark, process, upsert, **commit offsets, exit.**

| | Long-running consumer | **Batch drain (adopted)** |
|---|---|---|
| Scheduling authority | Kafka, implicitly | **Dagster** — one orchestrator |
| "Did week 39 run?" | Opaque; inspect consumer lag | **Asset materialization is the answer** |
| Failure mode | Silent death, lag creep | Failed asset run — visible and retryable in the Dagster UI |
| Idle cost | A process babysat 24/7 to do one thing per week | Nothing running between runs |
| Kafka still load-bearing? | Yes | **Yes** — the deliverable is real, not a tee |

**News is the exception** and keeps a genuine Structured Streaming job with its own checkpoint (it polls NAaaS every 15–30 min and its micro-batch cadence is real).

---

## Alternatives rejected

- **Full fidelity (everything through Spark Streaming):** using a streaming engine on one weekly PDF is pure ceremony; forces Spark skills on every contributor; the proposal text does not require it.
- **Minimal Kafka (news only):** contradicts the deliverables list (Kafka producers for all four source families); savings over the chosen option are marginal; and it splits the platform into two ingestion patterns, which is the entropy this ADR exists to avoid.
- **Long-running consumers for all sources:** rejected in A3 above.

## Consequences

- Only the NLP team needs Spark skills; ingestion students write ~50-line Python connectors.
- Two processing styles coexist (Spark job + Dagster batch drains) — mitigated by shared libs (schemas, provenance, gazetteer, epi-week) and by the fact that **Dagster is the single orchestrator for both**.
- Proposal/deliverable claims remain literally true and demoable for HEC review.
- **The justification is now honest**, which matters: a team that believes Kafka is load-bearing for throughput will reach for Spark clusters the next time a number looks big.
