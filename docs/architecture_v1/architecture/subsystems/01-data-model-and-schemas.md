# CHIP Subsystem 01 — Canonical Data Model & Schemas

**Status:** Design (v0.1) · **Owner:** Data Architecture · **Audience:** MS/PhD implementers
**Depends on:** ADR-001 (event-driven topology), ADR-002 (Kafka+Spark+Dagster), ADR-003 (Postgres/MinIO/Neo4j), ADR-004 (self-hosted Docker Compose), ADR-005 (district × epi-week grain + provenance)

This document is the *contract* every other subsystem builds against. It defines the canonical
gazetteer, the epidemiological-week library, the Kafka message envelope, the bronze/silver/gold
zone model, the PostgreSQL star schema, schema-evolution rules, and data-quality gates. It is an
engineering spec, not a proposal — where a choice exists we make it and give a one-line reason.

**Design priorities (from team reality):** correctness, auditability, maintainability over
throughput. Volume is modest (~160 districts, weekly bulletins, low-thousands articles/day).
Prefer boring, well-documented, Python-friendly tools. Every record is reproducible from bronze.

---

## 0. Cross-cutting conventions

- **Naming:** `snake_case` for SQL identifiers, Kafka fields, and Python. Tables are singular-noun
  prefixed by role: `dim_*`, `fact_*`, `bridge_*`, `stg_*` (staging), `raw_*` (landing).
- **IDs:** surrogate keys are `BIGINT GENERATED ALWAYS AS IDENTITY`. Natural/business keys are kept
  alongside as `*_code` or `*_natural_key` and are `UNIQUE`.
- **Time:** store all timestamps as `TIMESTAMPTZ` in UTC. Local reporting time (PKT, UTC+5) is
  derived on read. Dates that are calendar-only (a bulletin's epi-week) are `DATE`.
- **Text:** `TEXT` everywhere (no `VARCHAR(n)` guessing). Urdu is stored as UTF-8; the DB is
  `ENCODING 'UTF8'`, `LC_COLLATE 'und-x-icu'` (ICU collation so Urdu/mixed sorts sanely).
- **Versioning:** every transform stamps `transform_version` (semver string of the code module that
  produced the row). Bump it whenever output semantics change.
- **Provenance is mandatory** on every silver/gold row (§5.6).

---

## 1. Canonical gazetteer (spatial backbone)

### 1.1 Source of record — decision

**Use the OCHA/HDX Common Operational Dataset – Administrative Boundaries (COD-AB) for Pakistan,
dataset `cod-ab-pak`, as the authoritative gazetteer.** It is the de-facto humanitarian standard,
carries stable **P-codes**, ships both geometries and a tabular gazetteer, and is kept current
(the Pakistan COD-AB was last reviewed Sep 2024; live geoservices are hosted by ITOS/USAID).

- Dataset: `https://data.humdata.org/dataset/cod-ab-pak`
- Admin hierarchy for Pakistan:
  - `admin0` = country (Pakistan, `PK`)
  - `admin1` = province / territory (Punjab, Sindh, KP, Balochistan, ICT, AJK, GB) — 7 units
  - `admin2` = **district** ← **CHIP canonical grain (ADR-005)**
  - `admin3` = tehsil / taluka (kept for future drill-down, not the grain)
- P-code format: `PK` + numeric segments per level, e.g. `PK` / `PK1` (province) / `PK101`
  (district) — treat the P-code string as opaque and canonical; do not parse digits for hierarchy,
  join through the parent columns instead.
- Resources to ingest: the boundary layer (GeoPackage/Shapefile → PostGIS) **and** the tabular
  gazetteer (`pak_adminboundaries_tabulardata.xlsx`) which is the alias/name source.

**License note:** HDX COD-AB is published under an open license (verify the exact license field on
the dataset page before any redistribution — COD-AB is typically CC-BY-IGO / public-domain-
equivalent). Internal analytical use is unambiguously fine. Record the license string in
`dim_source` for the gazetteer feed.

**Secondary/enrichment sources (do not override COD-AB P-codes):**
- **GADM** and **geoBoundaries** — only to cross-check geometry and fill tehsil gaps.
- **PMD station registry** and **NIH reporting-unit list** — mapped *into* COD-AB districts via a
  crosswalk (§5.5), never used as the primary key space.

### 1.2 Table design

The gazetteer is a slowly-changing dimension. We keep the *current* view plus full history so a
2016 bulletin still resolves to the districts that existed in 2016.

```sql
-- Canonical location dimension (admin2/district grain, with hierarchy denormalized in)
CREATE TABLE dim_location (
    location_sk        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pcode              TEXT NOT NULL,                 -- COD-AB admin2 P-code, e.g. 'PK101'
    admin_level        SMALLINT NOT NULL DEFAULT 2,   -- 2 = district (canonical)
    name_en            TEXT NOT NULL,                 -- official English name
    name_ur            TEXT,                          -- official Urdu name (UTF-8)
    province_pcode     TEXT NOT NULL,                 -- admin1 parent P-code
    province_name_en   TEXT NOT NULL,
    -- SCD-2 validity window (when districts split/merge/rename)
    cod_ab_version     TEXT NOT NULL,                 -- e.g. '2024-09'
    valid_from         DATE NOT NULL,
    valid_to           DATE,                          -- NULL = current
    is_current         BOOLEAN NOT NULL DEFAULT TRUE,
    -- geometry (PostGIS)
    geom               GEOMETRY(MultiPolygon, 4326),  -- WGS84
    centroid           GEOMETRY(Point, 4326),
    -- provenance
    source_id          BIGINT NOT NULL REFERENCES dim_source(source_id),
    retrieved_at       TIMESTAMPTZ NOT NULL,
    transform_version  TEXT NOT NULL,
    UNIQUE (pcode, valid_from)
);
CREATE INDEX ix_dim_location_geom ON dim_location USING GIST (geom);
CREATE INDEX ix_dim_location_current ON dim_location (pcode) WHERE is_current;

-- Alias / transliteration table (many aliases -> one location)
CREATE TABLE location_alias (
    alias_sk       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_sk    BIGINT NOT NULL REFERENCES dim_location(location_sk),
    alias_text     TEXT NOT NULL,                     -- surface form seen in the wild
    alias_norm     TEXT NOT NULL,                     -- normalized key (see 1.3)
    script         TEXT NOT NULL CHECK (script IN ('latin','arabic','mixed')),
    alias_type     TEXT NOT NULL,                     -- 'official' | 'transliteration'
                                                      -- | 'historical' | 'colloquial'
                                                      -- | 'misspelling' | 'nih_unit' | 'pmd_station'
    confidence     REAL NOT NULL DEFAULT 1.0,
    source_id      BIGINT REFERENCES dim_source(source_id),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_alias_norm ON location_alias (alias_norm);
-- trigram index for fuzzy geocoder fallback
CREATE INDEX ix_alias_trgm ON location_alias USING GIN (alias_norm gin_trgm_ops);
```

### 1.3 Alias / transliteration handling (Urdu ⇄ English)

Pakistani place names have no single canonical romanization (Rawalpindi/Rawalpindindi;
راولپنڈی; "Pindi"; Faisalabad/Lyallpur historical). The geocoder resolves surface strings to a
`location_sk` through `location_alias`, in this priority order:

1. **Exact match** on `alias_norm` (deterministic).
2. **Trigram fuzzy match** (`pg_trgm`, similarity ≥ 0.6) for misspellings — returns candidates
   with a confidence score for human/LLM adjudication, never auto-committed below threshold.
3. **Unmatched → quarantine** (§6.3) with the raw string, for weekly curation into new aliases.

**Normalization function `alias_norm`** (implemented once in `libs/geonorm`, applied identically at
seed time and query time — this symmetry is the whole point):

- Unicode NFKC → casefold → strip diacritics/tashkeel → collapse whitespace/hyphens.
- Transliterate Arabic-script Urdu to a Latin key using a fixed table (seeded from the ITOS/UN
  romanization + a curated CHIP override list). Store both the original `alias_text` and the
  `alias_norm`; never destroy the original.
- Example: `"راولپنڈی"`, `"Rawalpindi"`, `"Pindi"` all normalize toward key `rawalpindi`.

Seed `location_alias` from three feeds: (a) COD-AB `name_en`/`name_ur` as `official`; (b) a
hand-curated CHIP alias CSV (checked into `data/gazetteer/aliases_seed.csv`, PR-reviewed); (c)
harvested-then-approved strings from the quarantine queue.

### 1.4 Versioning when districts change

Districts split (e.g. new districts carved from existing ones) roughly annually. Rules:

- **New COD-AB release → new `cod_ab_version` load**, run as a Dagster job. Diff against current:
  - unchanged district → keep row, bump nothing;
  - renamed → close old SCD-2 row (`valid_to`, `is_current=false`), insert new row, add old name as
    a `historical` alias;
  - split/merge → close parents, insert children, and write rows into `location_lineage`.

```sql
CREATE TABLE location_lineage (
    old_location_sk BIGINT NOT NULL REFERENCES dim_location(location_sk),
    new_location_sk BIGINT NOT NULL REFERENCES dim_location(location_sk),
    change_type     TEXT NOT NULL CHECK (change_type IN ('split','merge','rename','recode')),
    area_fraction   REAL,          -- for apportioning historical counts across a split
    effective_date  DATE NOT NULL,
    note            TEXT,
    PRIMARY KEY (old_location_sk, new_location_sk, effective_date)
);
```

Analytical marts (§4) resolve facts to the **district set valid on the fact's own date**, then
apply `location_lineage.area_fraction` when a mart needs a single stable panel across a boundary
change. This keeps historical bulletins correct without rewriting them.

---

## 2. Epidemiological-week library (`libs/epiweek`)

### 2.1 Which convention does NIH Pakistan use?

The NIH IDSR *Public Health Bulletin* labels reports **"Week N, YYYY" / "Epid Week N"** (confirmed
in Weekly_Report_26-2025) but does **not print the week's start/end dates**, so the convention must
be pinned by validation, not assumed. Two candidates:

- **CDC / MMWR** — week starts **Sunday**, ends Saturday; week 1 is the first week with ≥4 days in
  the calendar year.
- **WHO / ISO-8601** — week starts **Monday**, ends Sunday; week 1 contains the year's first
  Thursday.

**Decision:** default CHIP to the **WHO/ISO (Monday-start)** convention, because Pakistan's IDSR is
run under WHO/EMRO guidance and WHO's standard epidemiological week is Monday–Sunday. **But treat
this as validation-gated (Open Question OQ-1):** the library is convention-parameterized, and we
lock the default only after reconciling several known `(week, year)` labels against date ranges in
NIH/WHO-EMRO bulletins. Building both conventions in costs nothing and protects us if NIH turns out
to follow MMWR.

### 2.2 Library spec

Wrap the well-tested **`epiweeks`** PyPI package (supports both CDC and WHO/ISO systems) — do not
hand-roll week math. `libs/epiweek` is a thin, opinionated façade so the rest of CHIP never touches
raw week arithmetic.

```
libs/epiweek/
  __init__.py
  core.py          # thin wrapper over `epiweeks`
  config.py        # CHIP_EPIWEEK_SYSTEM = "WHO"  (default; "CDC" switchable via env)
  tests/           # golden table: (year, week) -> (start_date, end_date) for 2013..2026
```

```python
# libs/epiweek/core.py  — public surface
from datetime import date
from epiweeks import Week

CHIP_SYSTEM = "iso"  # "iso" == WHO/Monday-start ; "cdc" == MMWR/Sunday-start

def from_date(d: date, system: str = CHIP_SYSTEM) -> "EpiWeek": ...
def parse(year: int, week: int, system: str = CHIP_SYSTEM) -> "EpiWeek": ...

class EpiWeek:
    year: int          # epi-year (may differ from calendar year at boundaries)
    week: int          # 1..52/53
    system: str
    def start_date(self) -> date: ...          # inclusive
    def end_date(self) -> date: ...            # inclusive
    def epiweek_id(self) -> int:               # canonical surrogate: year*100 + week
        return self.year * 100 + self.week     # e.g. 202526
    def contains(self, d: date) -> bool: ...
    def __iter__(self): ...                    # yields the 7 dates
```

Rules the library enforces so every subsystem agrees:
- The **canonical week key is `epiweek_id = year*100 + week`** (`202526`), used as the FK across
  all facts and as the Kafka `epiweek` field.
- Weather/news observations at daily/sub-daily grain are assigned an `epiweek_id` via
  `from_date(observation_date)` — this is the single join key onto the disease panel.
- The library is the **only** place week↔date conversion happens; a golden-table unit test (§2.1)
  guards against silent convention drift.

---

## 3. Kafka message design

### 3.1 Envelope standard — CloudEvents

**Every** message on **every** topic (all sources ride Kafka per ADR-002) uses a **CloudEvents
1.0** envelope (structured-content mode). CloudEvents is a CNCF spec with mature Python tooling and
gives us tracing/provenance fields for free.

```json
{
  "specversion": "1.0",
  "id": "01J9Z6K3M2T4V8QW2R7X",
  "source": "/chip/connectors/nih-idsr",
  "type": "pk.chip.disease.case_report.v1",
  "subject": "PK101/202526/malaria",
  "time": "2026-07-08T06:12:00Z",
  "datacontenttype": "application/json",
  "dataschema": "apicurio:/apis/registry/v2/.../disease_case_report/versions/1",
  "data": {
    "pcode": "PK101",
    "epiweek_id": 202526,
    "disease_code": "MAL",
    "suspected_cases": 134,
    "confirmed_cases": null,
    "reporting_unit": "DHIS Rawalpindi",
    "provenance": {
      "source": "nih-idsr",
      "bronze_uri": "s3://chip-bronze/nih-idsr/2026/w26/Weekly_Report_26-2025.pdf",
      "retrieved_at": "2026-07-08T05:50:00Z",
      "transform_version": "nih-idsr-parser@1.4.0"
    }
  }
}
```

CHIP extension attributes (CloudEvents allows top-level extensions): `chip_epiweek` (int),
`chip_pcode` (string), `chip_traceid` (propagated end-to-end). These are duplicated at the envelope
level so consumers can route/filter without deserializing `data`.

### 3.2 Serialization — JSON Schema (not Avro)

**Decision: JSON Schema over Avro.** Rationale for *this* project:
- Volume is tiny; Avro's binary compactness buys nothing here.
- The team is Python-first and rotating; JSON is directly inspectable in `kafka-console-consumer`,
  MinIO, and logs — a huge debuggability/onboarding win.
- Our records are naturally nested/optional (news signals, nullable confirmed counts); JSON Schema
  expresses that with less friction than Avro unions.
- Postgres is JSON-native (`JSONB`), so bronze→silver stays lossless without a schema-language hop.

We still get **enforced, versioned, registry-backed schemas** — just in JSON Schema.

### 3.3 Schema registry — Apicurio (primary)

**Decision: Apicurio Registry**, Postgres-backed.
- **Apache-2.0 licensed** (no Confluent Community License ambiguity), and it can persist schemas in
  **PostgreSQL** — which ADR-003 already mandates — so we add a schema table set, not a new engine.
- Confluent-compatible REST API, so standard Kafka SerDes work unchanged.
- **Alternative (drop-in):** **Karapace** (Aiven, Apache-2.0, 1:1 Confluent-API compatible) if we
  prefer a Kafka-topic-backed store. Either is fine; pick Apicurio to reuse Postgres.
- **Confluent Schema Registry** is *legally usable* here (the Confluent Community License permits
  self-hosted internal use; we are not offering a competing SaaS), but we avoid it to keep the whole
  stack Apache-2.0 and telemetry-free. Documented, not chosen.

Compatibility policy is set **per subject** in the registry (§6.2): default **BACKWARD**.

### 3.4 Topic naming, keys, partitioning

**Topic naming:** `chip.<domain>.<source>.<entity>.v<major>`
```
chip.health.nih_idsr.disease_case_report.v1
chip.weather.pmd.station_obs.v1
chip.hazard.ndma.disaster_event.v1
chip.media.dawn.article_raw.v1
chip.media.tribune.article_raw.v1
chip.media.enriched.media_signal.v1     # Spark NLP output (§4)
chip.gazetteer.cod_ab.location_upsert.v1
```
- `<domain>` ∈ {health, weather, hazard, media, gazetteer}; `<source>` is the connector slug; major
  version in the topic name so a breaking schema change is a *new topic* (safe blue/green cutover).

**Keys & partitioning:**
- **Structured sources** (health/weather/hazard) key = **`pcode`** → all records for a district land
  in-order on one partition (weekly ordering matters for the panel).
- **Media raw** key = **`article_url_hash`** (dedup + idempotent re-ingest).
- **Media enriched** key = **`pcode`** (co-locate with structured facts for the KG builder).
- Partition count: **6 per topic** (ample headroom at our volume; keeps rebalance cheap). Do not
  over-partition — ordering per key is what we care about, not parallelism.

### 3.5 Retention & compaction per topic class

| Topic class | Example | Cleanup policy | Retention | Why |
|---|---|---|---|---|
| Raw event (append log) | `*.article_raw.v1`, `*.station_obs.v1` | `delete` | **long / effectively infinite** (bronze in MinIO is the true archive; Kafka keeps ~90d for replay) | Immutable events; MinIO is system-of-record |
| Derived/enriched | `media.enriched.media_signal.v1` | `delete` | 30–90d | Reproducible from bronze via Spark backfill |
| Compacted state | `gazetteer.cod_ab.location_upsert.v1` | `compact` | infinite | Latest-per-`pcode` is the useful state |
| Dead-letter | `*.dlq.v1` | `delete` | 180d | Long window for post-mortem/reprocessing |

Because MinIO bronze (§4) is the immutable system-of-record, Kafka retention is a *replay
convenience*, not durability — so we keep it modest and cheap.

### 3.6 Dead-letter topics

One DLQ per source topic: `chip.<domain>.<source>.<entity>.dlq.v1`. A message is dead-lettered when
it fails schema validation, geocoding (unresolvable `pcode`), or epiweek parsing. DLQ payload wraps
the original CloudEvent plus a failure envelope:

```json
{
  "failed_stage": "geocode",
  "error_code": "PCODE_UNRESOLVED",
  "error_detail": "alias 'Killa Saifullah' not in location_alias",
  "original_topic": "chip.health.nih_idsr.disease_case_report.v1",
  "original_offset": 84213,
  "first_seen_at": "2026-07-08T06:12:03Z",
  "retry_count": 0,
  "original_event": { ... }
}
```
Dagster runs a **DLQ drain job**: retriable errors (transient DB) are replayed; data errors
(unresolved alias) flow to the §6.3 quarantine queue for human/LLM curation, then replay.

---

## 4. Zone model (bronze / silver / gold)

Medallion architecture, mapped onto our three engines. **Lineage is recorded at every hop.**

```
 sources ──connectors──▶ Kafka ──▶ [BRONZE: MinIO raw immutable]
                                         │  (plain Python consumers / Spark for news)
                                         ▼
                                   [SILVER: Postgres normalized + geocoded + epiweek-tagged]
                                         │  (Dagster-orchestrated dbt/SQL + pandera)
                                         ▼
                                   [GOLD: Postgres analytical marts — district×epiweek panel]
                                         │
                                         ├──▶ Neo4j CHKG (materialized from silver/gold)
                                         └──▶ pgvector RAG chunks
```

### 4.1 Bronze — MinIO (raw, immutable)

Every fetched artifact is written **once, unmodified**, before any parsing. This is the audit root:
any silver/gold row must be re-derivable from a bronze object.

- **Buckets (one per lifecycle, not per source):** `chip-bronze` (raw), `chip-silver-exports`
  (optional Parquet snapshots), `chip-artifacts` (models, embeddings).
- **Object key layout (bronze):**
  ```
  s3://chip-bronze/<source>/<yyyy>/<mm>/<dd>/<retrieved_ts>__<content_hash>.<ext>
  e.g. s3://chip-bronze/nih-idsr/2026/07/08/20260708T0550Z__sha256-9f3a...c1.pdf
       s3://chip-bronze/pmd/2026/07/08/20260708T0600Z__sha256-11bd...ef.csv
       s3://chip-bronze/dawn/2026/07/08/20260708T0603Z__sha256-77aa...02.html
  ```
- **Immutability:** enable MinIO **object-lock (WORM)** on `chip-bronze` with a retention window;
  content-addressed by `sha256` so re-fetches are naturally idempotent (same hash → skip).
- **Sidecar metadata** (`.meta.json` next to each object, and mirrored to MinIO object tags):
  ```json
  {
    "source": "nih-idsr",
    "source_url": "https://www.nih.org.pk/.../Weekly_Report_26-2025.pdf",
    "retrieved_at": "2026-07-08T05:50:00Z",
    "content_sha256": "9f3a...c1",
    "content_type": "application/pdf",
    "bytes": 2838643,
    "connector_version": "nih-idsr-connector@0.9.2",
    "http_status": 200,
    "epiweek_hint": 202526
  }
```

### 4.2 Silver — Postgres (normalized)

Parsed, typed, **geocoded to `location_sk`**, **epiweek-tagged**, provenance-stamped, one row per
real-world observation. This is where JSON becomes relational and where **pandera checks run**
(§7). Staging tables `stg_*` hold the parser output; validated rows move into the `fact_*`/`dim_*`
schema (§5). Silver rows keep a `bronze_uri` pointer back to §4.1.

### 4.3 Gold — Postgres (analytical marts)

The headline mart is the **district × epi-week panel** — the join surface for LSTM/Prophet/GLM-lag
models and the dashboard.

```sql
-- Gold: one row per (district, epi-week); climate + disease + hazard + media aligned
CREATE MATERIALIZED VIEW mart_district_epiweek AS
SELECT
    l.pcode,
    l.name_en                         AS district,
    e.epiweek_id,
    e.start_date, e.end_date,
    -- disease block (from fact_disease_cases, pivoted to canonical diseases)
    dc.dengue_cases, dc.malaria_cases, dc.cholera_cases, dc.ari_cases,
    -- weather block (station->district aggregated, §5.5)
    w.temp_mean_c, w.temp_max_c, w.precip_mm_sum, w.humidity_mean_pct,
    -- hazard block
    h.active_hazard_types,            -- array e.g. {flood,heatwave}
    -- media signal block
    m.media_signal_count, m.media_risk_score,
    -- provenance rollup
    now() AS materialized_at
FROM dim_location l
CROSS JOIN dim_epiweek e
LEFT JOIN v_disease_by_district_epiweek dc USING (pcode, epiweek_id)
LEFT JOIN v_weather_by_district_epiweek  w  USING (pcode, epiweek_id)
LEFT JOIN v_hazard_by_district_epiweek   h  USING (pcode, epiweek_id)
LEFT JOIN v_media_by_district_epiweek    m  USING (pcode, epiweek_id)
WHERE l.is_current;
CREATE UNIQUE INDEX ON mart_district_epiweek (pcode, epiweek_id);
```

### 4.4 What transforms happen where, and lineage

| Hop | Where | Transform | Lineage record |
|---|---|---|---|
| source → bronze | connector (Python) | fetch, hash, WORM-write + sidecar | `.meta.json` + `raw_ingest_log` row |
| bronze → silver (structured) | Dagster + plain Python | parse, type, **geocode**, **epiweek-tag**, validate | `transform_run` row; `bronze_uri` on each fact |
| bronze → silver (news) | **Spark** (ADR-002) | NER/RE, HeidelTime normalize, entity-link → `fact_media_signal` | `transform_run` + per-signal `bronze_uri` |
| silver → gold | Dagster + SQL/dbt | aggregate to district×epiweek panel | `dbt` run artifacts + `materialized_at` |
| silver/gold → Neo4j | KG builder | project rows to CHKG nodes/edges | edge property `source_fact_sk` |

Lineage is captured in an explicit run-ledger, queryable for "why is this number here?":

```sql
CREATE TABLE transform_run (
    run_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_name          TEXT NOT NULL,          -- Dagster op / Spark app
    transform_version TEXT NOT NULL,
    input_zone        TEXT NOT NULL,          -- 'bronze'|'silver'
    output_zone       TEXT NOT NULL,
    input_ref         TEXT,                   -- bronze_uri or source view
    rows_in           BIGINT, rows_out BIGINT, rows_quarantined BIGINT,
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ,
    status            TEXT NOT NULL           -- 'success'|'partial'|'failed'
);
```

---

## 5. PostgreSQL schema (star schema)

Kimball-style star. Extensions required: `postgis`, `timescaledb`, `vector`, `pg_trgm`.
Conformed dimensions (`dim_location`, `dim_epiweek`, `dim_disease`, `dim_hazard_type`,
`dim_source`) are shared across all facts.

### 5.1 Dimensions

```sql
-- Epi-week dimension (materialized from libs/epiweek, one row per week 2013..present+1yr)
CREATE TABLE dim_epiweek (
    epiweek_id   INTEGER PRIMARY KEY,        -- year*100 + week, e.g. 202526
    epi_year     SMALLINT NOT NULL,
    epi_week     SMALLINT NOT NULL,
    start_date   DATE NOT NULL,
    end_date     DATE NOT NULL,
    system       TEXT NOT NULL DEFAULT 'iso',-- 'iso'(WHO) | 'cdc'
    is_53_week   BOOLEAN NOT NULL DEFAULT FALSE
);

-- Optional day grain for weather joins
CREATE TABLE dim_date (
    date_id   DATE PRIMARY KEY,
    epiweek_id INTEGER NOT NULL REFERENCES dim_epiweek(epiweek_id),
    dow       SMALLINT, month SMALLINT, year SMALLINT, is_monsoon BOOLEAN
);

-- Disease controlled vocabulary + ICD mapping
CREATE TABLE dim_disease (
    disease_sk    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    disease_code  TEXT NOT NULL UNIQUE,      -- CHIP canonical, e.g. 'DEN','MAL','CHOL','ARI'
    name_en       TEXT NOT NULL,
    nih_label     TEXT,                      -- exact IDSR bulletin label, e.g. 'AWD (S. Cholera)'
    category      TEXT NOT NULL,             -- 'vector-borne'|'water-food-borne'|'respiratory'|'vpd'
    icd10_code    TEXT,                      -- e.g. 'A90' (dengue)
    icd11_code    TEXT,                      -- e.g. '1D20'
    icd_mapping_confidence REAL DEFAULT 1.0,
    is_climate_sensitive BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dim_hazard_type (
    hazard_sk    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hazard_code  TEXT NOT NULL UNIQUE,       -- 'FLOOD','HEATWAVE','DROUGHT','SMOG'
    name_en      TEXT NOT NULL,
    hazard_class TEXT                        -- 'hydromet'|'climatological'|'air-quality'
);

CREATE TABLE dim_source (
    source_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_slug     TEXT NOT NULL UNIQUE,    -- 'nih-idsr','pmd','ndma','dawn','tribune','cod-ab'
    source_name     TEXT NOT NULL,
    custodian       TEXT,                    -- 'NIH','PMD','NDMA', ...
    modality        TEXT NOT NULL,           -- 'surveillance'|'weather'|'disaster'|'media'|'gazetteer'
    access_tier     TEXT NOT NULL,           -- 'public'|'historical'|'mou-pending'|'restricted'
    license         TEXT,                    -- license string (esp. gazetteer)
    cadence         TEXT                     -- 'weekly'|'daily'|'continuous'|'event'
);
```

**Disease vocabulary + ICD strategy.** CHIP's canonical `disease_code` is the join key; ICD-10 and
ICD-11 are *attributes*, not keys (institutions disagree on ICD versions). The NIH bulletin uses its
own labels (`AD (Non-Cholera)`, `AWD (S. Cholera)`, `ILI`, `ALRI <5 years`, `VH (B,C&D)`, etc.) —
store the exact bulletin string in `nih_label` and map it to `disease_code` via a reviewed
crosswalk (`data/vocab/disease_crosswalk.csv`). ICD mapping is **curated, not inferred**, with a
confidence column; phase-1 focus diseases (dengue/malaria/cholera/ARI) are mapped by hand first.

### 5.2 `fact_disease_cases` (grain: district × epi-week × disease)

```sql
CREATE TABLE fact_disease_cases (
    fact_sk          BIGINT GENERATED ALWAYS AS IDENTITY,
    location_sk      BIGINT NOT NULL REFERENCES dim_location(location_sk),
    epiweek_id       INTEGER NOT NULL REFERENCES dim_epiweek(epiweek_id),
    disease_sk       BIGINT NOT NULL REFERENCES dim_disease(disease_sk),
    suspected_cases  INTEGER,
    confirmed_cases  INTEGER,
    deaths           INTEGER,
    reporting_unit   TEXT,
    -- provenance (see 5.6)
    source_id        BIGINT NOT NULL REFERENCES dim_source(source_id),
    bronze_uri       TEXT NOT NULL,
    retrieved_at     TIMESTAMPTZ NOT NULL,
    transform_version TEXT NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (location_sk, epiweek_id, disease_sk, source_id),
    CONSTRAINT cases_nonneg CHECK (COALESCE(suspected_cases,0) >= 0)
);
```

### 5.3 `fact_weather` (grain: station × day, TimescaleDB hypertable)

Weather is genuinely time-series and high-cardinality → **TimescaleDB hypertable** on the raw
station grain; district aggregation is a continuous aggregate (§5.5).

```sql
CREATE TABLE fact_weather_station (
    station_id     TEXT NOT NULL,             -- PMD station code
    location_sk    BIGINT NOT NULL REFERENCES dim_location(location_sk), -- station's district
    obs_time       TIMESTAMPTZ NOT NULL,
    temp_c         REAL, temp_min_c REAL, temp_max_c REAL,
    precip_mm      REAL, humidity_pct REAL,
    source_id      BIGINT NOT NULL REFERENCES dim_source(source_id),
    bronze_uri     TEXT NOT NULL,
    retrieved_at   TIMESTAMPTZ NOT NULL,
    transform_version TEXT NOT NULL
);
SELECT create_hypertable('fact_weather_station', 'obs_time', chunk_time_interval => INTERVAL '30 days');
CREATE INDEX ON fact_weather_station (station_id, obs_time DESC);
```

Hypertable choice rationale: only `fact_weather_station` gets Timescale (real high-frequency
series). `fact_disease_cases` stays a plain table — weekly, ~160 districts × ~25 diseases is tiny;
a hypertable would add ops burden for no gain.

### 5.4 `fact_hazard_event` and `fact_media_signal`

```sql
-- Hazard events are episodic (a flood spans dates & districts) -> event grain + bridge to districts
CREATE TABLE fact_hazard_event (
    event_sk       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hazard_sk      BIGINT NOT NULL REFERENCES dim_hazard_type(hazard_sk),
    start_date     DATE NOT NULL, end_date DATE,
    severity       TEXT,                       -- controlled: 'minor'|'moderate'|'severe'
    headline       TEXT,
    source_id      BIGINT NOT NULL REFERENCES dim_source(source_id),
    bronze_uri     TEXT NOT NULL,
    retrieved_at   TIMESTAMPTZ NOT NULL,
    transform_version TEXT NOT NULL
);
CREATE TABLE bridge_hazard_location (
    event_sk    BIGINT NOT NULL REFERENCES fact_hazard_event(event_sk),
    location_sk BIGINT NOT NULL REFERENCES dim_location(location_sk),
    epiweek_id  INTEGER NOT NULL REFERENCES dim_epiweek(epiweek_id),
    PRIMARY KEY (event_sk, location_sk, epiweek_id)
);

-- Media signal: one row per (article, extracted claim), entity-linked to district+disease
CREATE TABLE fact_media_signal (
    signal_sk        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    article_url_hash TEXT NOT NULL,
    location_sk      BIGINT REFERENCES dim_location(location_sk),  -- geocoded, nullable
    epiweek_id       INTEGER REFERENCES dim_epiweek(epiweek_id),  -- HeidelTime-normalized
    disease_sk       BIGINT REFERENCES dim_disease(disease_sk),
    hazard_sk        BIGINT REFERENCES dim_hazard_type(hazard_sk),
    signal_type      TEXT,               -- 'outbreak_mention'|'hazard_mention'|'service_pressure'
    polarity         REAL,               -- extracted intensity/risk score
    snippet          TEXT,
    lang             TEXT NOT NULL,      -- 'en'|'ur'
    ner_model_version TEXT,
    source_id        BIGINT NOT NULL REFERENCES dim_source(source_id),
    bronze_uri       TEXT NOT NULL,
    retrieved_at     TIMESTAMPTZ NOT NULL,
    transform_version TEXT NOT NULL
);
```

### 5.5 Station → district aggregation

- **Assignment:** each PMD station is mapped to its containing district via
  `ST_Contains(dim_location.geom, station_point)` at load, cached in `fact_weather_station.location_sk`.
- **Aggregation to the panel:** a Timescale **continuous aggregate** rolls station-days into
  district-epi-weeks. Multiple stations per district are averaged for temp/humidity and **summed
  for precip** (physically correct); districts with **no station** are left NULL and flagged (later,
  optionally IDW-interpolated — see OQ-3). Coverage is a first-class quality metric (§7).

```sql
CREATE MATERIALIZED VIEW v_weather_by_district_epiweek
WITH (timescaledb.continuous) AS
SELECT location_sk,
       (SELECT epiweek_id FROM dim_date d WHERE d.date_id = time_bucket('1 day', obs_time)::date) AS epiweek_id,
       avg(temp_c) temp_mean_c, max(temp_max_c) temp_max_c,
       sum(precip_mm) precip_mm_sum, avg(humidity_pct) humidity_mean_pct,
       count(DISTINCT station_id) station_count
FROM fact_weather_station
GROUP BY location_sk, epiweek_id;
```
(If the continuous-aggregate epiweek expression proves awkward, compute `epiweek_id` at load into a
column and bucket on it — functionally identical.)

### 5.6 Mandatory provenance pattern

**Every** silver/gold fact carries this exact column set (copy-paste contract):

```sql
    source_id         BIGINT NOT NULL REFERENCES dim_source(source_id),
    bronze_uri        TEXT   NOT NULL,   -- exact MinIO object this row derives from
    retrieved_at      TIMESTAMPTZ NOT NULL,  -- when the source artifact was fetched
    transform_version TEXT   NOT NULL,   -- semver of the producing transform
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now()
```
A row without a resolvable `bronze_uri` is a bug (enforced by a pandera check). This makes ADR-005's
"every record carries provenance" literally true and every number traceable to an immutable object.

### 5.7 pgvector — RAG chunks

For the graph-RAG / evidence-summary layer. Chunks are embedded text from bronze documents, linked
back to source and (where known) to district/disease/CHKG nodes.

```sql
CREATE TABLE rag_chunk (
    chunk_sk       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    doc_bronze_uri TEXT NOT NULL,            -- source document in MinIO
    source_id      BIGINT NOT NULL REFERENCES dim_source(source_id),
    chunk_index    INTEGER NOT NULL,         -- position within doc
    char_start     INTEGER, char_end INTEGER,
    lang           TEXT NOT NULL,            -- 'en'|'ur'
    content        TEXT NOT NULL,
    token_count    INTEGER,
    -- entity links for graph-grounded retrieval
    location_sk    BIGINT REFERENCES dim_location(location_sk),
    epiweek_id     INTEGER REFERENCES dim_epiweek(epiweek_id),
    chkg_node_ids  TEXT[],                   -- Neo4j node ids this chunk supports
    -- embedding metadata
    embedding      VECTOR(1024),             -- dim MUST match the model; document it
    embed_model    TEXT NOT NULL,            -- e.g. 'bge-m3'
    embed_version  TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (doc_bronze_uri, chunk_index, embed_model)
);
CREATE INDEX ix_rag_embed ON rag_chunk USING hnsw (embedding vector_cosine_ops);
```
Chunking policy (documented so re-embeds are reproducible): sentence-aware, ~512-token windows with
~64-token overlap; **language-segmented** (never mix Urdu/English in one chunk); re-chunk/re-embed
is a versioned batch keyed by `embed_model`+`embed_version` so old and new embeddings coexist during
migration.

---

## 6. Schema evolution & drift strategy

The single biggest phase-2 risk (per the proposal): NIH/PMD/NDMA feeds arrive later in *different*
formats than the public/historical data we build on now. The architecture must absorb that without
rewrites.

### 6.1 Versioning model (three layers, decoupled)

1. **Wire schema** (Kafka/CloudEvents `dataschema`): major version in the **topic name**
   (`...v1`, `...v2`). Breaking change = new topic + parallel consumer, then cut over. Never mutate
   a live `vN` breaking.
2. **Parser/transform version** (`transform_version`): semver on the code that turns a specific
   source layout into canonical rows. A new institutional layout = a **new parser module** selected
   by a `layout_fingerprint`, not an edit to the old one (old bulletins must still reparse).
3. **Storage schema** (Postgres): forward-only migrations via **Alembic**; additive by default
   (new nullable columns), never rename-in-place.

### 6.2 Compatibility rules (registry, per subject)

- Default **BACKWARD** compatibility (new schema can read old data): you may **add optional fields**
  and widen types; you may **not** remove/rename required fields or narrow types within a major.
- Anything that would break BACKWARD → **bump the topic major version** (§6.1.1).
- CI gate: a schema PR runs Apicurio's compatibility check against the registered subject; red =
  blocked.

### 6.3 Quarantine flow (drift landing zone)

When a feed arrives mis-shaped (unknown columns, unresolved place, unparseable week, failed
validation), rows are **not dropped and not force-fit** — they land in quarantine:

```sql
CREATE TABLE quarantine_record (
    q_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id        BIGINT NOT NULL REFERENCES dim_source(source_id),
    bronze_uri       TEXT NOT NULL,
    stage            TEXT NOT NULL,          -- 'parse'|'geocode'|'epiweek'|'validate'|'schema'
    reason_code      TEXT NOT NULL,
    raw_payload      JSONB NOT NULL,         -- the offending record, verbatim
    layout_fingerprint TEXT,                 -- hash of detected structure, for drift clustering
    detected_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    status           TEXT NOT NULL DEFAULT 'open',  -- 'open'|'curated'|'wontfix'|'replayed'
    resolution_note  TEXT
);
```
Flow: connector/parser can't map → write `quarantine_record` (+ DLQ event) and continue. A weekly
Dagster **curation review** clusters by `layout_fingerprint`; recurring new layouts trigger a new
parser module (§6.1.2) or new aliases (§1.3); curated rows are **replayed** through the normal path.
`layout_fingerprint` clustering is what turns "NDMA changed their PDF" from an outage into a triaged
backlog item.

---

## 7. Data quality & validation

### 7.1 Tooling — pandera (primary), Great Expectations (optional, later)

**Decision: `pandera` as the default validation layer.**
- Schema-as-Python-code lives next to the transforms; runs inline in the bronze→silver step (and in
  Spark via `pandera`'s pyspark support) — no separate service, minimal ops for a rotating team.
- Fails fast, routes bad rows to quarantine (§6.3), and doubles as executable documentation of each
  fact's contract.
- **Great Expectations** is heavier (data docs, a store, more moving parts). Adopt it **only** later
  for stakeholder-facing data-quality dashboards if needed — not for phase 1. Don't run both as
  gates.

```python
# silver/schemas/disease_cases.py
import pandera as pa
from pandera.typing import Series

class DiseaseCasesSchema(pa.DataFrameModel):
    pcode: Series[str]      = pa.Field(str_matches=r"^PK\d+$")
    epiweek_id: Series[int] = pa.Field(ge=201301, le=209952)
    disease_code: Series[str] = pa.Field(isin=DISEASE_VOCAB)          # controlled vocabulary
    suspected_cases: Series[int] = pa.Field(ge=0, nullable=True)
    bronze_uri: Series[str] = pa.Field(str_startswith="s3://chip-bronze/")  # provenance is mandatory
    class Config:
        strict = "filter"   # unknown columns -> quarantine, not silent pass
```

### 7.2 Where checks run

- **Connector (bronze):** artifact-level — non-empty, expected `content_type`, hash recorded.
- **Silver (pandera gate):** the hard gate — types, ranges, controlled vocab, resolvable FKs,
  mandatory provenance. Failures → `quarantine_record`, counted into `transform_run.rows_quarantined`.
- **Gold (assertion queries):** panel-level sanity — no duplicate `(pcode, epiweek_id)`, disease
  totals reconcile to silver, weather coverage above threshold.

### 7.3 Freshness & completeness metrics (per source)

Written to a `dq_metric` table each run and surfaced on an ops panel:

| Source | Freshness SLO | Completeness metric |
|---|---|---|
| NIH IDSR | new bulletin ≤ 10 days after epi-week close | # districts reporting / # expected districts |
| PMD weather | daily obs ≤ 48h late | # districts with ≥1 station-day / 160 |
| NDMA/PDMA | event ingested ≤ 72h of publication | # events with resolved district |
| Dawn/Tribune | continuous, lag ≤ 6h | # articles geocoded / # articles ingested |

```sql
CREATE TABLE dq_metric (
    metric_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id    BIGINT NOT NULL REFERENCES dim_source(source_id),
    epiweek_id   INTEGER REFERENCES dim_epiweek(epiweek_id),
    metric_name  TEXT NOT NULL,        -- 'freshness_hours'|'completeness_pct'|'quarantine_rate'
    metric_value DOUBLE PRECISION NOT NULL,
    threshold    DOUBLE PRECISION,
    breached     BOOLEAN,
    measured_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## 8. Open questions

- **OQ-1 (epi-week convention):** Confirm empirically whether NIH IDSR bulletins follow **WHO/ISO
  (Mon-start)** or **CDC/MMWR (Sun-start)**. Bulletins print "Week N" without date ranges; resolve
  by reconciling several known `(week, year)` labels against WHO-EMRO/NIH dated material, or by
  asking NIH directly. Default is WHO/ISO but the lib is switchable. **Blocks locking `dim_epiweek`.**
- **OQ-2 (gazetteer license):** Record the exact HDX `cod-ab-pak` license string in `dim_source`
  before any external redistribution of derived boundaries; internal use is fine now.
- **OQ-3 (weather gaps):** Policy for districts with no PMD station — leave NULL vs IDW/kriging
  interpolation vs borrow-from-neighbor. Affects GLM-lag model inputs; decide with the modeling team.
- **OQ-4 (disease crosswalk granularity):** Should composite NIH labels (`VH (B,C&D)`,
  `AVH (A & E)`, `ALRI <5 years`) be split into ICD-precise diseases or kept as reported bins?
  Splitting risks fabricating precision the source doesn't have. Lean toward keeping bins, mapped to
  ICD *ranges*.
- **OQ-5 (institutional grain):** Whether incoming NIH/PMD feeds report at district (admin2) or a
  finer/coarser unit than COD-AB. If PMD reports by station-cluster or NIH by "reporting site,"
  we need a crosswalk table before their MOU data lands.
- **OQ-6 (news dedup & backfill grain):** Confirm `article_url_hash` is stable across Dawn/Tribune
  URL canonicalization (query strings, AMP variants) before relying on it as the media key.
- **OQ-7 (embedding dimension lock-in):** `VECTOR(1024)` assumes a specific multilingual model
  (e.g. bge-m3). Confirm the Urdu-capable embedding model with the NLP team before creating the
  HNSW index at scale (changing dim = table rebuild).

---
*End of Subsystem 01. Companion docs: 02-ingestion-connectors, 03-nlp-pipeline, 04-knowledge-graph-rag,
05-analytics-forecasting-alerting, 06-serving-dashboard, 07-infrastructure-operations.
Reconciliation ADRs that make this doc's contracts binding platform-wide: ADR-006 (spatial key),
ADR-007 (Kafka wire contract), ADR-008 (epi-week), ADR-009 (Apicurio registry), ADR-011 (embedding dim).*
