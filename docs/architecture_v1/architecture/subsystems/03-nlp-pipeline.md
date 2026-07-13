# Subsystem 03 — NLP Enrichment Pipeline

**Project:** CHIP (Climate–Health Intelligence Platform) — NRPU, PCN research group, NUCES/FAST Islamabad
**Subsystem owner:** NLP architecture
**Status:** Design (v1) — 2026-07, **revised 2026-07-13**
**Scope:** Extraction of epidemiologically meaningful signals from Urdu + English news and institutional reports; the only subsystem permitted to use Spark (per ADR-002).
**ADRs added 2026-07-13:** **ADR-011** (embeddings = BGE-M3, 1024-dim), **ADR-013** (news comes from the NAaaS API, not CHIP scrapers), **ADR-014** (2 × 24 GB GPUs, serving/batch split — **closes OQ-9**).

> ### ⚠️ Revision note (2026-07-13) — apply these three corrections throughout
>
> | Where this doc says… | …read it as | Why it matters |
> |---|---|---|
> | `multilingual-e5-base` / LaBSE (**768-dim**) — §1.3 table, §2.4 JSON `"embed"`, Appendix B | **`BAAI/bge-m3`, 1024-dim (ADR-011)** | `rag_chunk.embedding` is `VECTOR(1024)`. A dimension mismatch caught late = full table rebuild + re-embed; old and new vectors cannot share an HNSW index. |
> | `gadm_id` / `district_gadm` (`PAK.8.14_1`) — §2.4 JSON, Appendix A | **COD-AB admin2 `pcode` (ADR-006)** | GADM/GeoNames stay as *alias* sources for candidate generation; the **resolved output field is `pcode`**. §5.1 carries this note; the schema and worked example were missed. |
> | News arrives from CHIP **scrapers** on per-outlet topics (`raw.news.dawn`, …) — §0, §6.1 | **News arrives from the lab's NAaaS API** on one topic, `chip.media.naaas.article.v1` **(ADR-013)** | See §0.1 — this is the change that makes this subsystem's compute budget real. |

---

## 0. Where this subsystem sits

This is the **news NLP enrichment pipeline**. It consumes news documents off Kafka, produces structured, provenance-tagged records, and hands them to the CHKG builder and the forecasting/feature layer. It does **not** own ingestion, the knowledge graph, or forecasting — it produces the *feature substrate* they consume.

```
                                   ADR-002 boundary: Spark = news NLP only
 ┌───────────┐  Kafka           ┌───────────────────────────────────────────┐  Kafka          ┌──────────────┐
 │  naaas    │ ─ chip.media. ─▶ │      THIS SUBSYSTEM (Spark Structured      │ ─ enriched. ──▶ │ CHKG builder │
 │ connector │   naaas.         │      Streaming enrichment + backfills)     │  media_signal   │ (Neo4j)      │
 │ (ADR-013) │   article.v1     └───────────────────────────────────────────┘                 └──────────────┘
 └───────────┘                       │                 │                │
   ▲ ALREADY                  silver.* tables     pgvector          review queue
   │ RELEVANCE-FILTERED       (PostgreSQL)     (bge-m3, 1024-d)   (Label Studio tasks)
   │ by the keyword query
```

### 0.1 The relevance gate now lives upstream — and it is what makes this subsystem affordable

**This is the most consequential change of the 2026-07-13 review.** §3.1 has always said that *"only ~2–5% of the firehose is relevant — do not annotate raw random draws."* But the **pipeline** in §6.1 had **no such gate**: it ran language-ID, dedup, NER, RE, causal classification, geo-linking **and embedding** over *every* ingested article, and subsystem 04 then turned every one into a `:Document` node with `:Evidence` spans. Nobody had connected those two facts.

Under **ADR-013**, news is retrieved by a **keyword query** against NAaaS. **CHIP never ingests the irrelevant 95–98%.** The gate exists by construction — and it is what makes three separate sizings true rather than fictional:

| | Without a gate (the old design) | **With it (ADR-013)** |
|---|---|---|
| GPU — full enrichment of a 5-yr corpus | 2–6 GPU-**days** | **~hours** |
| pgvector | ~3M chunks ≈ **12 GB of vectors**; an HNSW build needs ~15–20 GB RAM against a 16 GB Postgres — **it would simply not build** | ~150k chunks ≈ **600 MB** |
| Neo4j CHKG | **5–10M nodes** — an order of magnitude past 04 §3.3's stated design point | **~2–3M nodes** — exactly the stated design point |

**Two obligations follow, and neither is optional:**

1. **Preserve the denominator.** 05 §2.4 normalises media surge by *district total news volume* — the entire point is to distinguish *"more disease news"* from *"more news."* A keyword-only feed supplies the numerator and **destroys the denominator**. **ADR-013 contract C1** (an unfiltered count endpoint on NAaaS) is what preserves it. Without C1, `media_surge_z` is uninterpretable and **the project's headline hypothesis — that news leads official surveillance — cannot be tested honestly.**
2. **Measure the gate's recall.** A relevance gate is a **false-negative risk**: an outbreak reported in unusual framing can be filtered out and never seen at all. Sample ~1% of NAaaS **non-matches** into the review queue and track the miss rate per disease and language. A silent filter is how you miss an outbreak; a *measured* filter is an engineered component with a known error rate.

Other sources (NIH IDSR, PMD, NDMA) do **not** pass through the Spark stream — they use **Dagster batch drains** (ADR-002 §A3). Institutional PDF reports (NIH/NDMA) *are* text and *do* need NER/temporal/geo; they are a **second document class** that reuses this subsystem's model-serving and geo/temporal services **but is orchestrated by Dagster batch jobs, not the Spark stream** (low-volume, weekly, not latency-sensitive). Design the extraction library so it is callable from both the Spark stream and a Dagster op.

Canonical output grain aligns to **district × epi-week** (ADR-005), with a `media_signal` record as the atomic emission. Provenance is mandatory on every derived record.

---

## 1. Language & Model Strategy

### 1.1 The honest 2026 situation for Urdu + English extraction

Verified state of resources (WebSearch, July 2026):

- **XLM-RoBERTa** remains the workhorse multilingual encoder for Urdu sequence labeling. Independent studies on Urdu/Indic NER report XLM-R fine-tuned reaching ~92 F1 on multilingual NER and topping most entity classes; **MuRIL** is essentially at parity (slightly better recall / OOV handling). Both beat mBERT. ([EDU-NER-2025](https://arxiv.org/pdf/2504.18142), [XLM-R vs MuRIL comparison](https://www.researchgate.net/figure/FINE-TUNNING-RESULTS-OF-XLM-ROBERTA-AND-MURIL-ON-THE-TEST-PART-OF-MULTI-LINGUAL-DATASET_tbl1_374448770))
- Urdu-specific fine-tunes exist but are thin and task-narrow (e.g. `hassan4830/xlm-roberta-base-finetuned-urdu`, Roman-Urdu variants). There is **no** authoritative domain (climate/health) Urdu NER model — we must build it. Newer massively-multilingual encoders (mmBERT, 2025) exist but buy us little for a two-language, domain-specific task.
- **GLiNER / GLiNER-multilingual** gives credible *zero-shot* span extraction across 40+ languages and beats general LLMs in zero-shot NER at a fraction of the cost — useful as a **cold-start** extractor and a pre-annotation aid before we have labels. ([GLiNER](https://arxiv.org/pdf/2311.08526), [GLiNER-MoE-Multilingual](https://huggingface.co/Mayank6255/GLiNER-MoE-MultiLingual))
- Self-hosted LLMs are now practical on one workstation GPU: a **Qwen3-30B-A3B (MoE) at Q4 fits ~17 GB**, and dense ~27–32B models fit ~17–20 GB at Q4 on a 24 GB RTX 4090-tier card, served by vLLM/LMDeploy with AWQ/INT4. ([Qwen VRAM tables 2026](https://willitrunai.com/blog/qwen-3-gpu-requirements)) Throughput is the constraint, not fit.

**Conclusion.** In 2026 the *best available* Urdu+English extractor for our budget is a **fine-tuned XLM-RoBERTa** for the online path, with a **self-hosted LLM as an offline teacher/bootstrapper/fallback**, not as the primary online extractor. Rationale for not making the LLM primary: at low-thousands of docs/day we *could* afford it, but (a) LLM span extraction is non-deterministic and hard to version/reproduce for a provenance-mandatory system, (b) 1–2 GPUs cannot also serve interactive RAG + batch LLM extraction + training, and (c) a distilled encoder is 20–100× cheaper per doc and gives calibrated per-span confidence we need for the review queue. The LLM earns its GPU time in annotation and hard-case fallback, where its quality-per-token is highest.

### 1.2 Reconciling with the proposal's BERT + RoBERTa commitment

The proposal names **BERT and RoBERTa** as fine-tuned deliverables. This is satisfied honestly and literally, not by hand-waving:

| Proposal term | What we ship | Why it is faithful |
|---|---|---|
| **RoBERTa (fine-tuned)** | **XLM-RoBERTa** fine-tuned for CHIP NER + RE — the primary production model | XLM-R *is* the RoBERTa architecture and pretraining objective at multilingual scale. A fine-tuned XLM-R is a fine-tuned RoBERTa. |
| **BERT (fine-tuned)** | **MuRIL** (BERT architecture, South-Asia-focused) fine-tuned as the benchmarked alternative + English-only **RoBERTa/BioClinicalBERT** on the English subset | MuRIL is a BERT-family encoder; fine-tuning it delivers the "BERT" line item and gives a genuine comparative study (thesis-able). |

So the deliverable list becomes: **fine-tuned encoder NER/RE models (XLM-R primary; MuRIL + English-RoBERTa as benchmarked comparators)**, with the LLM documented as an *annotation and distillation tool*, not a replacement. This keeps the funded promise intact while being defensible in 2026.

### 1.3 Model roles (decision table)

**Device column added 2026-07-13 per ADR-014** (2 × 24 GB; **GPU 0 = resident serving, never preempted; GPU 1 = batch/training, preemptible**):

| Role | Model | When it runs | **Device (ADR-014)** |
|---|---|---|---|
| **Primary online NER + RE** | XLM-RoBERTa-base/large fine-tuned (token-classification head + RE head) | Every *relevant* doc, in the Spark stream (via inference service) | **GPU 0** — resident, ~4 GB, batched |
| **Embeddings (pgvector)** | **`BAAI/bge-m3`, 1024-dim (ADR-011)** — *not* e5-base/LaBSE | Every *relevant* doc | **GPU 0** — resident, ~3 GB |
| **Graph-RAG serving LLM** | 7–14B AWQ (04 §6) | Interactive, working hours | **GPU 0** — resident, ~8–12 GB |
| **Benchmark comparators** | MuRIL, English RoBERTa/BioClinicalBERT | Eval harness only + optional English-doc route | **GPU 1** (offline) |
| **Cold-start / bootstrap NER** | GLiNER-multilingual (zero-shot label prompts) | Phase 0, before labels exist; also proposes spans for annotators | **GPU 1** (light) |
| **Annotation pre-labeler + teacher** | Self-hosted LLM, **Qwen3-30B-A3B Q4** (or dense ~27–32B AWQ) via vLLM | Offline batch: pre-annotation, silver-label generation for distillation | **GPU 1** — owns the card |
| **Causal-narrative + hard-case fallback** | Same LLM, constrained JSON prompt | Only for docs the encoder RE head flags low-confidence | **GPU 1** (offline micro-batch) |
| **XLM-R fine-tuning** | — | Periodic | **GPU 1** — owns the card |

> **The 30–32B model is the *teacher*, not the *server*.** ADR-014 pins the interactive RAG model at 7–14B on GPU 0 and keeps the 30B class on GPU 1 as an offline teacher. That is the right split independent of hardware: **the teacher should be big and slow (quality per token is what matters); the online path should be cheap and deterministic** (03 §1.1's own argument for encoder-primary). GPU 0's resident footprint totals ~15–18 GB — comfortable on one 24 GB card, and **never preempted by a student's fine-tune.**

**Distillation loop:** the LLM (teacher, GPU 1) labels a large unlabelled pool → we train/refresh XLM-R (student, GPU 1) → XLM-R serves online (GPU 0). This is the mechanism that lets a cheap encoder inherit LLM quality while staying deterministic and versionable.

---

## 2. Task Specifications

### 2.1 Entity type inventory

Ten types. `severity` on the label is bracketed only where it changes the schema; otherwise entity attributes carry it.

| # | Entity type | Definition / notes | Example (EN) | Example (UR) |
|---|---|---|---|---|
| 1 | `DISEASE` | Named disease / syndrome, incl. acronyms | "dengue", "cholera", "AWD" | ڈینگی، ہیضہ |
| 2 | `SYMPTOM` | Clinical sign/symptom, not a disease | "high-grade fever", "watery diarrhoea" | تیز بخار، اسہال |
| 3 | `CLIMATE_VAR` | Meteorological/climate variable or condition | "temperature", "humidity", "rainfall", "smog" | درجہ حرارت، بارش، سموگ |
| 4 | `HAZARD_EVENT` | Discrete climate/disaster hazard | "flood", "heatwave", "flash flooding", "drought" | سیلاب، ہیٹ ویو |
| 5 | `LOCATION` | Any place ref (district, tehsil, city, province, UC, hospital catchment) | "Sindh", "Larkana", "Karachi" | سندھ، لاڑکانہ |
| 6 | `ORGANIZATION` | Institution/authority/facility | "NIH", "NDMA", "Civil Hospital" | این آئی ایچ |
| 7 | `CASE_COUNT` | Numeric health quantity + unit/type | "48 new cases", "3 deaths" | 48 نئے کیسز |
| 8 | `TEMPORAL_EXPR` | Any time expression (feeds §4) | "last week", "since Monday", "12 August" | گزشتہ ہفتے |
| 9 | `DEMOGRAPHIC` | Affected population qualifier | "children under five", "elderly" | بچے |
| 10 | `INTERVENTION` | Response/measure | "fumigation drive", "OPD set up", "advisory issued" | فوگنگ مہم |

Design choices:
- `CASE_COUNT` is a span with structured attributes (`value`, `count_type ∈ {cases, deaths, admissions, suspected, confirmed}`, `polarity ∈ {new, cumulative, active}`). It is the highest-value / lowest-agreement type — annotate carefully.
- Keep `LOCATION` a single type; the **admin level** (district/tehsil/city/province) is resolved downstream by geo-linking (§5), not by the NER label — the model is bad at guessing admin level, the gazetteer is authoritative.
- No nested entities in v1 (flat BIO). Revisit if `CASE_COUNT` inside `DEMOGRAPHIC` ("48 children") proves lossy.

### 2.2 Relation type inventory

Binary, typed, directed relations over extracted entities. Kept deliberately small.

| Relation | Domain → Range | Meaning | Example |
|---|---|---|---|
| `located_in` | {DISEASE-mention, HAZARD_EVENT, CASE_COUNT, ORG} → LOCATION | ties a fact to a place | dengue **located_in** Rawalpindi |
| `during` | {DISEASE-mention, HAZARD_EVENT, CASE_COUNT} → TEMPORAL_EXPR | ties a fact to a time | 48 cases **during** last week |
| `reports_count` | CASE_COUNT → DISEASE | count is of this disease | 48 cases **reports_count** dengue |
| `affects` | {DISEASE, HAZARD_EVENT} → DEMOGRAPHIC | who is affected | heatwave **affects** elderly |
| `co_occurs` | {DISEASE, HAZARD_EVENT, CLIMATE_VAR} ↔ same | mentioned together in a health-relevant window (symmetric, weak) | flood **co_occurs** cholera |
| `causes_hypothesized` | {HAZARD_EVENT, CLIMATE_VAR} → {DISEASE, SYMPTOM, CASE_COUNT} | reporting asserts/implies a causal/triggering link | flooding **causes_hypothesized** rise in cholera |
| `response_to` | INTERVENTION → {DISEASE, HAZARD_EVENT} | measure taken because of | fumigation **response_to** dengue |

`causes_hypothesized` is the epidemiologically load-bearing one and the hardest. **Naming discipline:** it is *hypothesized/reported* causality — CHIP never asserts causation, it records that a *source narrative* asserts it. The CHKG stores it as a claim with provenance, never as ground truth.

### 2.3 Event extraction & causal-narrative detection

Two layers:

**(a) Event framing.** A `media_signal` event = a coherent (disease|hazard, location, time, count?) tuple assembled from entities + `located_in`/`during`/`reports_count` relations. This is deterministic assembly from the RE output — no separate event model in v1. One document can emit multiple events.

**(b) Causal-narrative classifier.** For each candidate `(cause_entity, effect_entity)` pair in the same/adjacent sentence, classify the *reporting stance*:

```
causality_class ∈ {
   EXPLICIT   — source states the link with a causal cue
                ("dengue cases surged BECAUSE OF stagnant floodwater")
   IMPLIED    — juxtaposition/temporal-sequence implies it, no explicit cue
                ("After a week of flooding, hospitals saw a spike in diarrhoea.")
   CORRELATIONAL — source explicitly hedges ("may be linked", "associated with")
   NONE       — co-mention only, no causal reading
}
```

Plus `direction` (which is cause), `polarity` (increase/decrease), and `confidence`. Implementation:
- **v1 (rule + encoder):** a cue lexicon (EN + UR: "because of/due to/led to/triggered/caused/بارش کے باعث/کی وجہ سے") gates EXPLICIT/CORRELATIONAL; an XLM-R sentence-pair classifier over the entity pair handles IMPLIED vs NONE.
- **v2 (LLM fallback):** pairs the encoder marks low-confidence are re-scored by the self-hosted LLM with a constrained-JSON prompt returning `{class, direction, polarity, evidence_span, confidence}`. Only low-confidence pairs go to the LLM → bounded GPU cost.
- Store the **evidence span** always — this is what makes CHKG edges auditable/explainable (proposal requirement).

### 2.4 Enriched-document output JSON schema

One record per source document, written to the silver layer and emitted to `enriched.news`. Abbreviated but complete:

```jsonc
{
  "doc_id": "dawn:2026-07-08:a1b2c3",          // stable, source-derived
  "source": { "outlet": "Dawn", "url": "...", "section": "national" },
  "provenance": {                               // MANDATORY (ADR-005)
    "ingested_at": "2026-07-08T06:12:00Z",
    "dct": "2026-07-08",                        // document creation time, anchors §4
    "pipeline_run_id": "spark-2026-07-08T06:15Z-0007",
    "transform_version": "nlp@1.4.0",           // ties to MLflow, enables replay §7
    "model_versions": {
      "lang_id": "fasttext-lid-176@1",
      "ner": "xlmr-chip-ner@2.3",
      "re": "xlmr-chip-re@1.1",
      "causal": "xlmr-causal@0.9+llm-fallback@qwen3-30b-a3b-q4",
      "temporal": "heideltime@2.2.1+urdu-rules@0.3",
      "geo": "gazetteer@2026.06",
      "embed": "bge-m3@1"                       // ADR-011: 1024-dim (NOT e5-base/768)
    }
  },
  "language": { "primary": "ur", "mixed": true, "confidence": 0.97 },
  "text": { "title": "...", "body": "...", "char_len": 3820 },
  "entities": [
    { "id": "e1", "type": "DISEASE",  "text": "ڈینگی", "span": [102,107], "norm": "dengue", "cui": "C0011311", "conf": 0.98 },
    { "id": "e2", "type": "CASE_COUNT","text": "48 نئے کیسز", "span": [130,142],
      "attrs": {"value": 48, "count_type": "cases", "polarity": "new"}, "conf": 0.91 },
    { "id": "e3", "type": "LOCATION", "text": "لاڑکانہ", "span": [160,167], "conf": 0.95,
      "geo": { "pcode": "PK6xx", "district_en": "Larkana", "province": "Sindh",   // ADR-006: COD-AB
                                                                                  // admin2 P-code, NOT gadm_id
               "admin_level": "district", "lat": 27.56, "lon": 68.21,
               "link_conf": 0.88, "review_status": "auto" } },
    { "id": "e4", "type": "TEMPORAL_EXPR", "text": "گزشتہ ہفتے", "span": [90,99],
      "timex": { "type": "DATE", "value": "2026-W27", "value_norm": "2026-07-01/2026-07-07",
                 "resolver": "urdu-rules", "anchored_to": "dct" } }
  ],
  "relations": [
    { "type": "reports_count", "head": "e2", "tail": "e1", "conf": 0.90 },
    { "type": "located_in",    "head": "e1", "tail": "e3", "conf": 0.93 },
    { "type": "during",        "head": "e2", "tail": "e4", "conf": 0.87 }
  ],
  "causal_narratives": [
    { "id": "c1", "cause": "HAZARD_EVENT:flood", "effect": "e1",
      "class": "IMPLIED", "direction": "cause->effect", "polarity": "increase",
      "evidence_span": [200,268], "resolver": "llm-fallback", "conf": 0.72 }
  ],
  "media_signals": [                             // canonical atomic emission
    { "signal_id": "dawn:...:s1", "disease": "dengue", "hazard": null,
      "pcode": "PK6xx", "epi_week": "2026-W27",          // ADR-006 (was: district_gadm)
      "count": {"value": 48, "type": "cases", "polarity": "new"},
      "causal_link": "c1", "signal_conf": 0.85 }
  ],
  "embedding_ref": "pgvector:news_doc_emb:doc_id",
  "flags": { "needs_geo_review": false, "needs_causal_review": true, "dedup_of": null }
}
```

The `media_signal[]` array is the contract with CHKG + forecasting. Everything else is evidence/audit trail.

---

## 3. Annotation Program

### 3.1 Corpus sampling strategy

Goal: a labelled set that is *representative of health-relevant reporting*, not of the raw news firehose (99% of which is irrelevant). **Stratified + active + event-anchored sampling:**

1. **Relevance pre-filter** — a cheap keyword/embedding filter (disease + climate lexicon, then e5 similarity to seed paragraphs) to pull the candidate pool. Only ~2–5% of the firehose is relevant; do not annotate raw random draws.
2. **Stratify** the candidate pool by: language (UR / EN / code-mixed), outlet, disease family (vector-borne / water-borne / respiratory / heat), hazard type, and season (monsoon vs non-monsoon). Enforce Urdu ≥ 45% of tokens — Urdu is where models are weakest and where the funded novelty lives.
3. **Event-anchored oversampling** — deliberately include known outbreak/hazard windows (2022 floods, dengue peaks, smog season) so causal narratives are actually present.
4. **Active learning after round 1** — once a v0 model exists, sample by *disagreement/low-confidence* to spend annotation on hard cases (long tail of `CASE_COUNT`, IMPLIED causality).

### 3.2 Tooling — Label Studio (self-hosted)

- **Label Studio**, containerized on the lab servers (ADR-004), Postgres-backed, behind the university VPN. One project per task family (NER, RE, causal) so guidelines and label configs stay clean; link tasks by `doc_id`.
- **ML backend** connector points at the same inference service (§6) so **model-assisted pre-annotation** is one-click: GLiNER/LLM proposes spans, annotators *correct* rather than label from scratch. This is the single biggest throughput lever.
- Export in Label Studio JSON → converter to CoNLL/BIO (NER), JSONL pair format (RE/causal). Keep the converter in the repo; it is part of the reproducible pipeline.

### 3.3 LLM-assisted pre-annotation loop

```
 unlabelled doc ──▶ LLM/GLiNER pre-label (spans+relations+causal, constrained JSON)
                       │
                       ▼
              Label Studio task (pre-filled)
                       │  human corrects (accept/edit/reject)
                       ▼
              adjudication (2 annotators + curator on conflicts)
                       │
                       ▼
        gold set  +  correction telemetry (which classes the LLM gets wrong)
```

Guardrail: **never auto-accept LLM labels into gold.** Pre-annotation is a speed aid; every gold span is human-confirmed. Track LLM precision/recall per type from the accept/edit rates — it tells you where the encoder will struggle too.

### 3.4 Guidelines skeleton

One living doc per task, versioned in git alongside the label config. Skeleton:

1. Purpose & the epi question each type serves.
2. Per-entity: definition, positive examples, **hard negatives**, boundary rules (include modifiers? "high-grade fever" vs "fever"), Urdu-specific script/diacritic conventions, transliteration handling.
3. `CASE_COUNT` attribute decision tree (new vs cumulative, suspected vs confirmed) — the most error-prone.
4. Relation attachment rules (nearest-mention, cross-sentence limits).
5. Causal stance decision tree with the EXPLICIT/IMPLIED/CORRELATIONAL/NONE examples from §2.3, incl. Urdu causal cues.
6. Code-mixing rules (label in situ; language tag at token level).
7. Adjudication protocol + changelog.

### 3.5 IAA targets

| Task | Metric | Target | Floor to ship |
|---|---|---|---|
| NER (entity spans) | span-level F1 between annotators / Cohen's κ | κ ≥ 0.80 | 0.75 |
| `CASE_COUNT` attributes | Cohen's κ | ≥ 0.75 | 0.70 |
| Relations | F1 on relation triples | ≥ 0.75 | 0.70 |
| Causal stance (4-way) | Krippendorff's α | ≥ 0.67 | 0.60 |

Causal stance is inherently subjective — a 0.60–0.67 α is honest and acceptable; report it openly. Run a calibration round of ~50 docs double-annotated before scaling, recompute IAA every ~500 docs.

### 3.6 Dataset sizes for credible fine-tuning

| Split | Docs | Rationale |
|---|---|---|
| NER/RE train | 1,500–2,500 relevant docs (~30–50k entity spans) | Enough to fine-tune XLM-R for domain NER given strong pretraining + LLM-distilled silver augmentation |
| Gold dev | 250 docs | Model selection |
| Gold test (frozen) | 300–400 docs, ≥45% Urdu, event-balanced | Reported metrics, never trained on |
| Causal-stance | 1,500–2,000 entity pairs | 4-way classifier |
| **Silver (LLM-distilled)** | 20k–50k docs, unlabelled→LLM-labelled | Distillation augmentation; not counted as gold |

Encoder fine-tuning needs *hundreds–low-thousands* of gold docs, not tens of thousands — pretraining does the heavy lifting. Distillation silver closes the rest. This is achievable with the rotating-student annotation program in one project year.

### 3.7 Thesis alignment (addresses the funded risk of student churn)

Carve annotation + a model into **self-contained thesis units** so a departing student leaves a finished artifact:

- MS thesis A: *Urdu–English climate–health NER corpus + XLM-R vs MuRIL benchmark* (owns §1–3 corpus + comparator study).
- MS thesis B: *Causal-narrative detection in Pakistani health reporting* (owns §2.3, the α study, LLM-vs-encoder comparison).
- MS thesis C: *District geo-entity linking with Urdu transliteration* (owns §5).
- PhD thread: *LLM-teacher → encoder-student distillation for low-resource epidemiological IE* (owns the §7 lifecycle + drift).

Each thesis = a gold dataset + a model + an eval table, all committed to the repo so the pipeline survives rotation.

---

## 4. Temporal Normalization

### 4.1 HeidelTime integration — run it as a JVM sidecar, not inside Spark

HeidelTime is Java/UIMA and needs a POS tagger (TreeTagger by default). Verified facts (WebSearch): HeidelTime has hand-crafted resources for **13 languages — Urdu is not one of them** (only auto-generated, low-quality resources cover it); and the popular Python wrappers (`py-heideltime`, `python-heideltime`) are **inactive/discontinued** and drag in TreeTagger+Perl. ([HeidelTime](https://github.com/HeidelTime/heideltime), [py-heideltime PyPI](https://pypi.org/project/py-heideltime/))

**Decision: do NOT embed a JVM in Spark executors and do NOT depend on the abandoned wrappers.** Run HeidelTime as a **containerized REST sidecar**:

```
 Spark executor (Python UDF)                 Temporal sidecar (container, ADR-004)
 ┌───────────────────────────┐   HTTP POST   ┌──────────────────────────────────────┐
 │ temporal_normalize(text,  │ ────────────▶ │  thin JVM service (Spring/Javalin)   │
 │   dct, lang)              │   {text,dct}  │   wraps HeidelTime JAR + TreeTagger   │
 │  routes by language ──────┼──────┐        │   → TimeML/TIMEX3 out                 │
 └───────────────────────────┘      │        └──────────────────────────────────────┘
                                     │        (EN/AR resources only; NOT Urdu)
                                     ▼
                        Urdu → Urdu-rules resolver (Python, in-executor)
                        low-conf → LLM normalization (offline batch)
```

Why sidecar over JVM-in-Spark: keeps executors pure-Python (simpler, no `jpype`/classpath hell across the cluster), lets us scale/restart the tagger independently, and isolates the abandoned-dependency risk in one container we control. At low-thousands of docs/day the HTTP round-trip is trivial; batch the calls per micro-batch partition.

We wrap the **HeidelTime JAR directly** (build our own ~150-line Javalin service) rather than the dead PyPI wrapper — no Perl, pin TreeTagger, reproducible container.

### 4.2 The Urdu gap and the fallback

HeidelTime handles English (and Arabic script gives a partial assist, but is unreliable) — **Urdu temporal expressions need our own resolver.** Three-tier strategy:

1. **English text →** HeidelTime sidecar (high quality).
2. **Urdu text →** a **rule-based Urdu TIMEX resolver** (Python): a curated pattern set for Urdu date/relative expressions — gregorian + Islamic month names, numerals (Urdu + Arabic-Indic digits), relative refs (گزشتہ ہفتے "last week", کل "yesterday/tomorrow", رواں ماہ "this month"), and day names. Normalize to TIMEX3 + ISO, anchored to DCT. This is a bounded, high-value, thesis-able artifact.
3. **Low-confidence / unparsed →** self-hosted **LLM normalization** (offline batch): prompt returns `{type, value (ISO/epi-week), anchor}` for the residual. LLM handles the messy long tail rules can't.

Every timex carries `resolver ∈ {heideltime, urdu-rules, llm}` and `anchored_to` for provenance and later error analysis.

### 4.3 Document-creation-time anchoring

Relative expressions ("last week", "گزشتہ ہفتے") are meaningless without an anchor. **DCT = article publish date** (from the scraper's metadata; fall back to first-seen ingest date, flagged lower-confidence). All relative TIMEX3 resolve against DCT, then snap to **epi-week** (ISO week, the ADR-005 canonical temporal grain). Absolute dates ignore DCT. Store both the raw normalized value and the epi-week bucket.

---

## 5. Geo Entity Linking

### 5.1 Gazetteer

> **Reconciled (ADR-006):** the canonical district key is the **COD-AB admin2 P-code** (subsystem 01 §1),
> not `gadm_id`. This linker's candidate-generation may still use GADM/GeoNames alternate-name tables as an
> alias source, but its **resolved output field is `pcode`**. `gadm_id` values in the examples below are
> illustrative and each resolves to a P-code via the alias table.

Build a **district-level gazetteer** as a PostGIS table, canonical to GADM 4.1 admin boundaries (Pakistan has ~145 districts excluding territories, ~171 including AJK/GB — we cover all; verified WebSearch). Columns: `gadm_id, name_en, province, division, admin_level, geom (polygon), centroid, population (prior), aliases[]`.

Alias sources (this is the whole game for Pakistan):
- Official English names + common English spellings ("Rawalpindi"/"Pindi").
- **Urdu script** names (راولپنڈی).
- **Transliteration variants** — Pakistani place names have many romanizations (Faisalabad/Lyallpur, D.G. Khan/Dera Ghazi Khan, Muzaffargarh/Muzaffar Garh). Seed from GADM + Geonames alternate names + a hand-curated list, extend as the review queue surfaces misses.
- Sub-district hooks: major tehsils/cities mapped to their parent district so "Gujar Khan" → Rawalpindi district.

### 5.2 Candidate generation → disambiguation → scoring

```
 LOCATION span "لاڑکانہ" / "Larkana"
      │
      ▼
 (1) CANDIDATE GEN
     - exact alias match (EN + UR)                      exact  → high base score
     - normalized/transliterated match (uroman/ICU)     fuzzy  → rapidfuzz token_set_ratio
     - phonetic match (double-metaphone on romanized)   phon   → catches spelling drift
      │  → candidate set {gadm_id, match_type, string_sim}
      ▼
 (2) DISAMBIGUATION (rank candidates)
     signals combined into a score:
       s = w1*string_sim
         + w2*admin_context   (other LOCATIONs in doc: if "Sindh" present, prefer Sindh districts)
         + w3*population_prior (log-pop; Karachi beats a tiny namesake)
         + w4*source_prior     (outlet's regional focus / dateline)
         + w5*cooccur_prior    (district that co-occurs with the doc's disease historically)
      ▼
 (3) CONFIDENCE
     link_conf = softmax margin between top-1 and top-2 candidate
      │
      ├── link_conf ≥ 0.85            → auto-link (review_status="auto")
      ├── 0.60 ≤ link_conf < 0.85     → auto-link + flag needs_geo_review=true
      └── link_conf < 0.60 OR no cand → NO link, push to review queue
```

Ambiguity is real in Pakistan (multiple "Kot"/"Jampur"/duplicated names across provinces) — the **admin-hierarchy context** signal (does the article also name a province/division?) plus **population priors** resolve most of it. Keep weights in config, tune on the gold geo set.

### 5.3 Human-review queue

Low-confidence links become **Label Studio geo-review tasks**: reviewer picks the right district from the top-k candidates (map + list). Every correction is written back as a **new alias** in the gazetteer → the system self-improves and the same string never needs review twice. This closes the loop and is cheap for rotating students to run.

```jsonc
// review task
{ "doc_id":"...", "span_text":"جیکب آباد", "romanized":"Jacobabad",
  "candidates":[
     {"gadm_id":"PAK.8.9_1","name_en":"Jacobabad","province":"Sindh","score":0.71},
     {"gadm_id":"PAK.7.3_1","name_en":"Jafarabad","province":"Balochistan","score":0.66}],
  "context_locations":["Sindh","Sukkur"], "suggested":"PAK.8.9_1" }
```

---

## 6. Pipeline Engineering (Spark Structured Streaming)

### 6.1 Stage graph

> **Reconciled (ADR-007 + ADR-013):** the source topic is **one** topic, `chip.media.naaas.article.v1`
> (CloudEvents-enveloped). The per-outlet topics shown in the original draft (`raw.news.dawn`, …) are
> **retired** — outlet is a *field* on the NAaaS record, not a topic. Output is
> `chip.media.enriched.media_signal.v1`.

```
 Kafka: chip.media.naaas.article.v1        ← ONE topic. ALREADY relevance-filtered (§0.1).
        │
        ▼  Spark Structured Streaming (micro-batch, ADR-002: Spark = news NLP only)
 ┌──────────────────────────────────────────────────────────────────────────────────────┐
 │ [0] DECODE + validate schema (JSON Schema) ──▶ malformed to ...naaas.article.dlq.v1    │
 │ [1] LANGUAGE ID  (fastText lid.176, in-executor, CPU)  → {ur|en|mixed}                 │
 │     + RE-APPLY Urdu confusable normalisation (do NOT trust upstream — 02 §2.4):        │
 │       a mis-normalised ی/ي silently breaks district geo-linking at [7]                 │
 │ [2] CROSS-OUTLET STORY CLUSTERING (stateful, MinHash/LSH over a 7-day watermark)       │
 │     ⚠ SKIP THIS STAGE ENTIRELY if NAaaS already returns a wire-story cluster id        │
 │       (02 OQ-5 — ASK THEM). NAaaS owns exact + near-dup; CHIP owns only cross-outlet   │
 │       wire copy (same PPI/APP story in Dawn AND Tribune), which inflates media_mentions│
 │       and biases the headline feature. This stage is the ONE genuinely Spark-shaped     │
 │       thing in the pipeline (stateful + watermarked).                                   │
 │ [3] NER          ── call INFERENCE SERVICE (XLM-R) ─────────────┐                       │
 │ [4] RE           ── call INFERENCE SERVICE (XLM-R RE head) ─────┤  batched calls to     │
 │ [5] CAUSAL       ── encoder classifier; low-conf → LLM batch ───┘  Triton on GPU 0      │
 │ [6] TEMPORAL     ── EN→HeidelTime sidecar | UR→rules | resid→LLM (§4)                   │
 │ [7] GEO-LINK     ── candidate gen + disambiguation vs gazetteer (§5) → resolves to      │
 │                     COD-AB `pcode` (ADR-006), at the doc-date's boundary vintage (ADR-015)│
 │ [8] EMBED        ── BGE-M3 (1024-dim, ADR-011) via inference service → vector           │
 │ [9] ASSEMBLE     ── build media_signals[], attach provenance + transform_version        │
 └──────────────────────────────────────────────────────────────────────────────────────┘
        │                          │                         │                    │
        ▼                          ▼                         ▼                    ▼
  silver.news_enriched     Kafka: enriched.news        pgvector:               review queue
  (PostgreSQL/Timescale)   (→ CHKG builder)            news_doc_emb            (Label Studio)
  + silver.media_signals   enriched.media_signal                               geo + causal
```

Stages 1–2 and 6–7 are CPU/IO and run in-executor (pandas UDFs). Stages 3–5, 8 are GPU and go through the **inference service** (below). `foreachBatch` writes the four sinks transactionally-ish: Postgres upsert keyed by `(doc_id, transform_version)`; Kafka publish; pgvector upsert; review tasks.

### 6.2 Micro-batch sizing

Low-thousands of docs/day ≈ tens of docs/minute peak. **Trigger every 30–60 s** (`Trigger.ProcessingTime`). Target micro-batch ~50–300 docs so GPU calls batch efficiently (encoder likes batch 16–32). `maxOffsetsPerTrigger` caps batch size to protect the GPU during backlogs/backfills. This is explicitly **micro-batch, not continuous** — latency of a minute is fine; nobody needs sub-second climate-health signals, and micro-batch keeps GPU utilization high.

### 6.3 GPU inference integration — inference service, NOT in-executor models

**Decision: Spark calls an external inference service; it does not load models into executors.** Given only 1–2 GPUs (ADR-004):

| | In-executor models | **External inference service (chosen)** |
|---|---|---|
| GPU sharing | each executor wants a GPU → can't fit on 1–2 GPUs | one service owns the GPU(s), batches across all executors |
| Batching | per-partition, poor | cross-request dynamic batching (Triton/vLLM) → high utilization |
| Model updates | redeploy Spark job | hot-swap model in service, Spark unchanged |
| Reuse | Spark-only | Dagster PDF path + Label Studio pre-annotation reuse the same service |
| Failure isolation | model OOM kills executor | isolated, retryable |

Architecture: **Triton Inference Server** hosting XLM-R NER/RE + the **BGE-M3** embedder (ADR-011) on **GPU 0**; Spark executors call it over gRPC/HTTP with retry + circuit-breaker. The teacher LLM runs as a **separate vLLM service on GPU 1**, used only by the offline causal/temporal fallback and distillation batches.

```
   Spark executors ──gRPC(batch)──▶ Triton  (GPU 0 — RESIDENT SERVING, never preempted)
                                      │  xlmr-ner, xlmr-re, xlmr-causal, bge-m3 (1024-d)
                                      │  + vLLM: graph-RAG LLM (7–14B AWQ)
                                      │  dynamic batching, model versions pinned  → ~15–18 GB
                                      │
   offline causal/temporal batch ──▶ vLLM  (GPU 1 — BATCH & TRAINING, preemptible)
   distillation / fine-tuning    ──▶       qwen3-30b-a3b-q4 teacher; XLM-R fine-tunes;
   historical enrichment backfill ─▶       GNN training. Owns the whole card.
```

> **ADR-014 closes the contention question that used to live in OQ-9.** The two workloads are on **two physical devices**, so there is no preemption policy to write and no scheduler to maintain: a student's fine-tune OOMing on GPU 1 **cannot** touch the serving stack on GPU 0. That failure isolation — not raw VRAM — is why 2 × 24 GB beats 1 × 48 GB here. "A student's training job crashed the stakeholder dashboard" is exactly the failure a rotating-team project cannot afford.
>
> **If only 1 GPU is fundable at month 0:** time-slice as an *interim* — encoders + BGE-M3 resident (small, ~7 GB), a 7–8B RAG model by day, LLM batch jobs nightly. This breaks the moment the historical enrichment backfill runs *while* the dashboard must be live, which is precisely when you demo. Buy the second card before Phase 2.

### 6.4 Outputs

- **silver.news_enriched** (Postgres): full enriched JSON (§2.4) as JSONB + typed columns for hot fields; keyed `(doc_id, transform_version)` so re-enrichment (§7) never overwrites history.
- **silver.media_signals** (Timescale hypertable): the atomic signals, `district × epi-week` grain, ready for forecasting joins.
- **Kafka enriched.news / enriched.media_signal**: for the CHKG builder (Neo4j) — decoupled, replayable.
- **pgvector**: doc + signal embeddings for RAG retrieval and near-dup/semantic search.

### 6.5 Backfills / replay

Same code path with a **bounded (batch) read** from Kafka or MinIO raw archive instead of the streaming source — this is the "backfills" clause of ADR-002. Backfill runs stamp a distinct `pipeline_run_id` and the current `transform_version`; idempotent upsert keys prevent double-counting.

---

## 7. Model Lifecycle

### 7.1 Registry & versioning — MLflow

- **MLflow** (self-hosted, Postgres backend + MinIO artifact store — reuses ADR-003 infra) is the single registry for every model: XLM-R NER/RE, MuRIL comparator, causal classifier, embedder, plus the Urdu-rules resolver version and gazetteer snapshot (tracked as versioned artifacts even though not ML).
- Each promoted model gets a stage (`Staging`→`Production`) and a **semantic `transform_version`** (`nlp@1.4.0`) that is a *pinned bundle* of all component versions. The enriched-doc `provenance.model_versions` records the exact component set; `transform_version` is the human-facing rollup.
- Datasets (gold train/dev/test) are versioned too (DVC or MLflow dataset logging) so every metric is reproducible from `(model_version, dataset_version)`.

### 7.2 Re-enrichment / replay (ties to provenance)

When a model improves:

```
 new model → MLflow Staging → eval harness (§7.3) gate
      │  passes gate
      ▼
 bump transform_version → deploy to inference service
      │
      ▼
 REPLAY: backfill job (§6.5) re-reads raw docs from MinIO,
         re-enriches under new transform_version,
         writes NEW silver rows (doc_id, transform_version=new)  ← old rows kept
      │
      ▼
 CHKG builder + forecasting choose which transform_version to consume
 (default: latest Production; old retained for audit + A/B)
```

Because provenance is mandatory and silver rows are keyed by `transform_version`, **re-enrichment is additive, not destructive** — you can always diff old vs new extraction, and downstream can pin a version for reproducibility. This is the payoff of the ADR-005 provenance mandate.

### 7.3 Evaluation harness

- **Frozen gold test set** (§3.6), ≥45% Urdu, event-balanced, never trained on. Versioned.
- **Metrics:** per-entity-type precision/recall/**F1** (span-level, exact + relaxed boundary); RE triple F1; causal-stance macro-F1 + per-class; **geo-linking accuracy@1 and accuracy@k** against gold district IDs; temporal value-accuracy (normalized value matches). Report **Urdu vs English broken out** — an aggregate number hides the Urdu weakness we most care about.
- **Gate:** a new model must not regress overall F1 and must not regress Urdu F1 by >1 point to reach Production. Automated as an MLflow-driven CI job.
- Slice reports: by outlet, by disease family, by season — surfaces where to spend the next annotation round.

### 7.4 Drift monitoring

- **Input drift:** track distribution of language mix, entity-density, OOV rate, and new-disease/new-location string rates per week; alert on shift (e.g. a new outbreak vocabulary).
- **Confidence drift:** mean/median model confidence and the *rate of docs hitting the review queue* — a rising review rate is the earliest, cheapest drift signal.
- **Label drift (delayed):** the human corrections coming out of the review queue are a continuous, free labelled stream — periodically score the live model against them → live precision estimate without a fresh annotation campaign.
- Retrain trigger: review-rate or live-precision crosses threshold, or every N months, whichever first. Retrain → §7.2 replay.

---

## 8. Open Questions

### Closed since the 2026-07-13 review

| Was | Now |
|---|---|
| ~~OQ-5 Gazetteer authority & boundary churn~~ | **Closed by ADR-015.** Subsystem 01 §1.2/§1.4 already had it: SCD-2 `dim_location` (`valid_from`/`valid_to`) + `location_lineage` with population-weighted `area_fraction`. Facts resolve against **the boundary vintage valid on the fact's own date**; marts declare an `analysis_vintage`. Data-model (01) owns it. |
| ~~OQ-8 Dedup across outlets~~ | **Mostly closed by ADR-013**, and now a *question for NAaaS* rather than a design problem. NAaaS owns exact + near-duplicate detection at collection. CHIP owns only **cross-outlet wire-story clustering** (§6.1 stage [2]) — **and only if NAaaS does not already return a cluster id (02 OQ-5: ask them).** If it does, stage [2] is deleted. |
| ~~OQ-9 GPU contention policy~~ | **Closed by ADR-014.** No policy is needed because there is no contention: **GPU 0 = resident serving (never preempted), GPU 1 = batch/training (owns the card).** Two physical devices, isolated by `CUDA_VISIBLE_DEVICES`. No scheduler, no preemption rules, no runbook. |

### Still open

1. **LLM primacy revisit.** We chose encoder-primary for determinism/throughput. If annotation stalls and gold stays thin, is an **LLM-primary online path with schema-constrained decoding + caching** acceptable, accepting the provenance/reproducibility cost? Needs a throughput + reproducibility spike on the real GPU. *(Note: ADR-012 establishes the pattern for exactly this trade — an LLM in the path is acceptable **iff** its output is cached immutably and keyed by version. The same trick would apply here.)*
2. **HeidelTime longevity.** The Python wrappers are abandoned and HeidelTime itself is quiet. Keep the JVM sidecar, or go **rules + LLM for both languages** and retain HeidelTime only as an English baseline? Depends on the English TIMEX quality delta in our eval.
3. **Urdu gold ceiling.** Can rotating MS students realistically produce 1,500–2,500 Urdu-inclusive gold docs in year 1 at κ ≥ 0.75? If not, how far do LLM-distilled silver + GLiNER cold-start carry us before F1 plateaus?
4. **`causes_hypothesized` epistemics.** Where does the CHKG draw the line between a *reported* causal claim and an *asserted* one, and does forecasting ever consume `IMPLIED`/`CORRELATIONAL` links, or only `EXPLICIT`? Joint decision with the CHKG (04) + forecasting (05) owners.
5. **Sub-district signal.** Reports frequently name tehsils/UCs/hospitals below district grain. We roll up to district (ADR-005) — but do we *retain* the finer location for future re-aggregation, and does retaining it risk indirect re-identification (the proposal's own ethics concern)?
6. **Code-mixed inference.** One XLM-R pass per doc, or split UR/EN segments? Single-pass is simpler; measure whether it costs Urdu F1 on code-mixed sentences.
7. **⭐ NEW — relevance-gate recall (§0.1).** The keyword query is now the *only* thing standing between CHIP and 95–98% of the news firehose. **What is its false-negative rate?** An outbreak reported in unusual framing ("mystery illness", "unknown fever", a local Urdu colloquialism) could be filtered out and never seen. Requires: a labelled recall eval, a ~1% sample of NAaaS non-matches routed to the review queue, and a lexicon-expansion loop. **A silent filter is how you miss an outbreak.** This is the cost of the gate and it must be paid deliberately.
8. **⭐ NEW — does NAaaS coverage match CHIP's needs?** CHIP's outlet list, language coverage, district coverage, and freshness are now **NAaaS's** (ADR-013). Is its district-level reporting deep enough for a 160-district panel, or is it metro-biased? A geographic coverage audit is a Phase-1 prerequisite — **an Urdu/English coverage asymmetry across districts would bias the media signal geographically** (already flagged as 05 OQ-4).

---

## Appendix A — Annotated snippet (worked example)

Source (Urdu, Dawn, DCT = 2026-07-08):
> «لاڑکانہ میں گزشتہ ہفتے شدید بارشوں اور سیلابی پانی کے باعث ہیضے کے 48 نئے کیسز رپورٹ ہوئے۔»
> *(Larkana: last week, due to heavy rains and floodwater, 48 new cases of cholera were reported.)*

Extraction:
- Entities: `LOCATION[لاڑکانہ]→PAK.8.14_1 (Larkana, link_conf 0.88)`, `TEMPORAL_EXPR[گزشتہ ہفتے]→2026-W27 (urdu-rules, anchored dct)`, `CLIMATE_VAR[شدید بارشوں]`, `HAZARD_EVENT[سیلابی پانی/flood]`, `DISEASE[ہیضے]→cholera`, `CASE_COUNT[48 نئے کیسز]{value:48,type:cases,polarity:new}`.
- Relations: `reports_count(48→cholera)`, `located_in(cholera→Larkana)`, `during(48→2026-W27)`.
- Causal: `causes_hypothesized(flood→cholera)`, `class=EXPLICIT` (cue "کے باعث"/"due to"), `polarity=increase`, evidence span = whole clause.
- media_signal: `{disease:cholera, hazard:flood, district:PAK.8.14_1, epi_week:2026-W27, count:{48,cases,new}, causal:EXPLICIT, conf:0.85}`.

## Appendix B — Config sketches

Inference service model bundle (pinned to a transform_version):
```yaml
# transform_version: nlp@1.4.0
serving:
  triton_url: grpc://infer-svc:8001      # GPU 0 — resident serving (ADR-014)
  vllm_url:   http://llm-svc:8000        # GPU 1 — offline causal/temporal fallback only
models:                                  # all resident on GPU 0, ~15-18 GB total
  ner:      { name: xlmr-chip-ner, version: "2.3", max_batch: 32 }
  re:       { name: xlmr-chip-re,  version: "1.1", max_batch: 32 }
  causal:   { name: xlmr-causal,   version: "0.9", lowconf_threshold: 0.55, fallback: llm }
  embed:    { name: bge-m3, version: "1", dim: 1024, max_batch: 64 }   # ADR-011 — NOT e5-base(768)
llm_fallback:
  model: qwen3-30b-a3b   # Q4 AWQ, ~17GB — TEACHER, on GPU 1, off-peak batch. Not the RAG server.
  device: cuda:1         # ADR-014: never GPU 0. A fine-tune OOM must not kill serving.
  schedule: "0 2 * * *"
```

Spark stream trigger + geo weights:
```python
(spark.readStream.format("kafka")
   .option("subscribe", "chip.media.naaas.article.v1")   # ADR-013: ONE topic, already
                                                         #   relevance-filtered upstream (§0.1)
   .option("maxOffsetsPerTrigger", 300)            # cap micro-batch → protect GPU 0
   .load()
   .transform(pipeline_stages)                     # §6.1 stage graph
   .writeStream.trigger(processingTime="45 seconds")
   .foreachBatch(write_silver_kafka_pgvector_reviewq)
   .option("checkpointLocation", "minio://chip/chk/nlp-enrich")
   .start())

GEO_WEIGHTS = dict(string_sim=0.40, admin_context=0.25,
                   population=0.15, source=0.10, cooccur=0.10)
GEO_AUTO_LINK, GEO_REVIEW_FLOOR = 0.85, 0.60
```

Urdu temporal rule (illustrative):
```python
URDU_RELATIVE = {
  "گزشتہ ہفتے": ("WEEK", -1), "اس ہفتے": ("WEEK", 0),
  "کل": ("DAY", -1), "رواں ماہ": ("MONTH", 0), "گزشتہ ماہ": ("MONTH", -1),
}  # resolve against DCT → ISO value → snap to epi-week
```
