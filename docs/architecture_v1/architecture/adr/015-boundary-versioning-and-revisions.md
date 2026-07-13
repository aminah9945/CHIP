# ADR-015: District-Boundary Versioning Is Settled (01 §1.4) — and Revisions Are Measured, Not Assumed

- **Status:** **Accepted** (2026-07-13)
- **Closes:** 02 OQ-3, 03 OQ-5, 04 OQ-1 (three subsystems independently raising *the same* unanswered question about district boundary changes) and **downgrades** 05 OQ-3 (vintage availability) from a design blocker to a measurement.

## Part A — Boundary versioning: the design already exists; use it

### The problem the three OQs were describing

Pakistan created and re-drew districts between 2021 and 2025. If a tehsil split out of Sialkot in 2023, then *"Sialkot"* in a 2022 IDSR bulletin and *"Sialkot"* in a 2025 bulletin are **different geographies**. A naive panel silently contains an undeclared structural break — and a DLNM will happily absorb that break as a real climate–disease effect. This corrupts the gold panel, which is the product.

### The resolution: it is not an open question — subsystem 01 solved it and nobody noticed

**Subsystem 01 §1.2 and §1.4 already specify the full mechanism.** ADR-015 does not design anything new; it **pins the seam** and supersedes the three open questions that were re-asking it:

1. **`dim_location` is a slowly-changing dimension (SCD-2)**, not a static lookup. It carries `cod_ab_version`, `valid_from`, `valid_to`, `is_current`, with `UNIQUE (pcode, valid_from)`.
2. **`location_lineage`** records every `split` / `merge` / `rename` / `recode` with an `area_fraction` for apportioning historical counts and an `effective_date`.
3. **Facts resolve to the district set valid on the fact's own date** (01 §1.4). A 2022 bulletin resolves against 2022 boundaries. Historical bulletins are **never rewritten**.
4. **Analytical marts that need one stable panel across a boundary change apply `location_lineage.area_fraction`** to project onto a declared **analysis vintage**.

### What ADR-015 adds on top

- **Every analytical query and every model card must declare its `analysis_vintage`** (the COD-AB version whose boundaries the panel is projected onto). Default: the current COD-AB version. This makes the boundary choice an explicit, auditable model input rather than an accident of when the mart was built.
- **`area_fraction` defaults to population-weighted, not area-weighted**, for apportioning disease counts across a split. Cases follow people, not hectares. Area-weighting a split district would systematically misallocate cases away from the urban core. Record which method was used per lineage row.
- **A gold-layer assertion check** (01 §7.2) fails the build if any fact resolves to a `dim_location` row whose validity window does not contain the fact's date.

### Ownership

**Data-model subsystem (01) owns the gazetteer, the lineage table, and the crosswalk.** Subsystems 02 (ingestion), 03 (geo-linking), 04 (CHKG `Location` nodes) and 05 (panel) are **consumers**, and their open questions on this topic are closed by this ADR. The gazetteer remains a **Phase-0 deliverable** — nothing downstream can be built correctly before it exists.

---

## Part B — IDSR revisions: instrument it, don't assume it

### The question

05 §2.5 builds an entire bitemporal / as-of-join apparatus (`value`, `valid_week`, `knowledge_time`) to prevent vintage leakage — training on revised counts you would not have had at decision time. 05 OQ-3 then asks, unresolved: *do NIH archives preserve provisional values, or only latest-revised?*

The project lead's current belief is that **IDSR bulletins do not reprint prior weeks' revised numbers.**

### Why "we'll find out during KG building" does not work

**The CHKG is built *from* the gold panel** (04 §2.1 — `DiseaseObs` reads Postgres gold). If the normalizer overwrote week 39's count when a later bulletin restated it, **the revision was destroyed before the graph ever saw it.** The graph cannot recover information the ingestion layer discarded. By the time relations are established, the evidence is gone.

### Decision: detect revisions at the normalizer UPSERT

The place a revision becomes visible is the **only** place a value is ever overwritten — the idempotent UPSERT keyed by `(location_sk, epiweek_id, disease_sk, source_id)`. Instrument it:

```sql
CREATE TABLE ingestion_revision (
    revision_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_sk       BIGINT  NOT NULL REFERENCES dim_location(location_sk),
    epiweek_id        INTEGER NOT NULL REFERENCES dim_epiweek(epiweek_id),
    disease_sk        BIGINT  NOT NULL REFERENCES dim_disease(disease_sk),
    source_id         BIGINT  NOT NULL REFERENCES dim_source(source_id),
    field             TEXT    NOT NULL,        -- 'suspected_cases' | 'confirmed_cases' | 'deaths'
    old_value         INTEGER,
    new_value         INTEGER,
    -- the bulletin that carried each value: this IS the vintage record
    old_bronze_uri    TEXT NOT NULL,
    new_bronze_uri    TEXT NOT NULL,
    old_retrieved_at  TIMESTAMPTZ NOT NULL,
    new_retrieved_at  TIMESTAMPTZ NOT NULL,
    detected_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Rule:** on UPSERT into `fact_disease_cases`, if an existing row's value differs from the incoming value, write an `ingestion_revision` row **before** overwriting. Cost: one trigger, ~10 lines. It commits us to nothing.

Run it across the historical backfill and for the first weeks of live ingest. Then we **know**, empirically, with zero assumptions:

| If `ingestion_revision` is… | Then | Consequence |
|---|---|---|
| **Empty** (the lead's expectation) | NIH publishes a **single vintage** — the first-reported value — and never restates it | **05 §2.5's bitemporal machinery is largely unnecessary.** There is no revision, therefore no leakage: training data, decision-time data, and target are all the same (provisional) vintage. This is a **simplification**. |
| **Non-empty** | NIH *does* restate, and each bulletin is a dated snapshot of what was known then | The archive of weekly PDFs **is** a complete vintage record. `knowledge_time` = the publishing bulletin's epi-week. 05 §2.5 is fully populatable and must be honoured. |

### The consequence nobody has stated: what CHIP is actually forecasting

**If revisions do not exist, CHIP forecasts *first-reported* counts, not truth.** That must be said out loud, in three places:

- **05 §3.4's outbreak gold standard** is currently defined on *"retrospective, revision-mature counts."* If no mature vintage exists, the gold standard must be redefined on provisional counts, or on NIH's own declared-outbreak records where they exist. **This changes every operational metric**, so it must be settled before the evaluation harness is frozen.
- **Every model card** states the vintage of its target.
- **Every stakeholder-facing forecast** is labelled as a forecast of *reported* cases, not of *true incidence*. Under-reporting is a property of the surveillance system, not a bug in the model, and conflating them is how a platform loses institutional trust.

### The parser consequence

**If** revisions do turn out to exist, the IDSR parser must extract **each bulletin's full retrospective table**, not just its current-week row. Parsing only the current week throws away the vintage record permanently and makes 05's backtest protocol unrecoverable. This is a **cheap insurance policy** — extract the whole table regardless, and let `ingestion_revision` tell us whether it mattered.

## Alternatives rejected

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **Instrument the UPSERT; measure (chosen)** | Answers the question with data, not belief; ~10 lines; commits to nothing; the historical backfill answers it for free | Requires the parser to read full tables (cheap insurance) | **Chosen** |
| Assume no revisions; drop the bitemporal design | Simpler now | If wrong, every backtest is silently optimistic and unrecoverable — the exact failure 05 §2.5 was written to prevent | Rejected |
| Assume revisions; build the full bitemporal apparatus regardless | Safe | Significant complexity that may serve nothing; and it *still* would not tell you whether it was needed | Rejected — measure first, then build only what the data justifies |
| "Find out during KG building" | No work now | **Structurally impossible.** The KG reads the panel; the panel already overwrote the evidence. | Rejected |
