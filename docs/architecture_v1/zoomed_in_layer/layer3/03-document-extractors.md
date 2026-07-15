# CHIP — Layer 3: Document Extractors

**Zoomed-in layer:** Layer 3 — Document Extraction
**Parent document:** New layer inserted between Block 2 (Connectors) and Block 4 (Harmonizers)
**Status:** Design (prototype phase — MD table extraction only)
**Audience:** MS/PhD implementers building and extending extractors
**Last updated:** 2026-07-15

> This document defines the extractor subsystem: the set of source-format-aware adapters that read raw documents from bronze, parse structured data out of them, and produce source-native records onto Kafka for the normalizer layer. This layer exists because the connector archives entire documents untouched, and different downstream consumers need different extractions from the same bronze artifact.

---

## 0. Architecture: Where the Extractor Fits

### 0.1 The full pipeline

```
Layer 2                      Layer 3                            Layer 4
Connector                    Extractor                           Normalizer
═════════                    ═════════                           ══════════
                             ┌─ nih_idsr_extractor ──→           ┌─ Disease Harmonizer
nih_idsr ──→ bronze ──┬─────┤ pitb_dss_extractor ──→ Kafka ──→  │
pitb_dss ──→ bronze ──┤     ├─ ajk_idsrs_extractor ──→          └─ Weather Harmonizer
ajk_idsrs ──→ bronze ─┤     └─ dhis_punjab_extractor ──→        ┌─ Hazard Harmonizer
(weather) ──→ bronze ─┼──── weather_json_extractor ──→          └─ NLP Pipeline
(ndma) ──→ bronze ────┼──── disaster_pdf_extractor ──→
(naaas) ──→ bronze ───┘
```

The extractor is the format boundary. To its left, everything is source-native bytes. To its right, everything is typed, structured records.

### 0.2 What the extractor DOES (and doesn't)

| Extractor responsibility | NOT the extractor's job |
|---|---|
| Find data tables in documents (structural parsing) | Map disease labels to canonical codes (normalizer) |
| Extract rows with source-native field names and values | Resolve district names to P-codes (normalizer) |
| Basic type checks: integer parseable, week in 1–53 | Map dates to epi-week IDs (normalizer) |
| Produce source-native records to Kafka | Reconcile totals across tables from the same document (normalizer — needs all records collected first) |
| Update `extractor_status` on success/failure | UPSERT into `fact_disease_cases` (normalizer) |
| Emit original labels exactly as they appear in the source | Range validation or plausibility checks (normalizer) |

**The extractor understands document structure, not data semantics.** It produces records that look exactly like the source document. The normalizer does the rest.

### 0.3 Extractors are format adapters

Different source formats need different extraction strategies. This is the same Template Method pattern as connectors: the SDK provides the shared infrastructure (poll `raw_documents`, produce to Kafka, update status), and each extractor fills in the source-specific parsing logic.

| Extractor type | Input format | Exists in prototype? | Example sources |
|---|---|---|---|
| **MD table extractor** | Markdown with HTML tables | ✅ Yes | NIH, PITB-DSS, AJK, DHIS |
| **JSON API extractor** | Structured JSON | 🔜 Future | Open-Meteo weather, NAaaS news |
| **PDF document extractor** | Binary PDF (agentic parse, ADR-012) | 🔜 Future | NDMA sitreps, live NIH PDFs |
| **CSV dataset extractor** | Delimited tabular text | 🔜 Future | OpenDengue, HDX datasets |

This document covers the MD table extractor type in full. The other types will follow the same SDK pattern when their source data arrives.

### 0.4 Per-source extractors

All 4 prototype sources are MD with HTML tables, but their table structures are fundamentally different:

| Source | Tables to find | Table orientation | Format eras |
|---|---|---|---|
| NIH IDSR | Province summary (Table 1) + 3 district tables (Tables 2–4) | Transposed (2021–2022) → Pivoted (2023–2026) | Two eras, incompatible layouts |
| PITB-DSS | Communicable disease situation + disease×district matrix | Transposed (diseases=rows, districts=cols) | Single layout per era |
| AJK IDSRS | Compliance + overall cases + district detail + weekly comparison | Pivoted (districts=rows) | Single layout |
| DHIS Punjab (2022) | OPD tables + epidemic disease table | Pivoted (districts=rows), ALL CAPS | DHIS-II era only |

**Decision: one extractor per source.** Each is ~80 lines. Adding a source means a new extractor, not editing a shared one.

### 0.5 Multiple extractors per document (future)

In the future, the same NIH bulletin will be processed by multiple extractors:

```
nih_idsr bronze artifact
    │
    ├── nih_idsr_extractor (disease tables) ──→ Kafka: chip.health.nih_idsr.disease_case_report.v1
    │
    ├── nih_idsr_prose_extractor (narrative)  ──→ Kafka: chip.media.nih_idsr.document_chunk.v1
    │
    └── nih_idsr_compliance_extractor (DQ)    ──→ Postgres: dq_metric (compliance rates)
```

This is why the `extractor_status` table tracks per-extractor processing state (§2.1).

### 0.6 Design priorities

1. **Extractor stays at source-native granularity.** Output = exactly what the source says, in the source's own labels.
2. **New source = new extractor.** No shared parsing. Template Method pattern: shared SDK, per-source parsing.
3. **Replay from bronze without re-fetch.** Any extractor improvement replays by re-processing pending rows.
4. **Format-change resilience.** A layout change quarantines the extractor, not the connector.

---

## 1. Extractor SDK (`libs/chip_extractors`)

### 1.1 SDK vs. extractor — what goes where

```
SDK (libs/chip_extractors/)               EXTRACTOR (extractors/nih_idsr/)
══════════════════════════════             ════════════════════════════════
✅ base.py — Extractor ABC                ✅ extractor.py — NihIdsrExtractor(Extractor)
✅ runner.py — run_extractor()            │  └─ extract() — parse MD, find tables,
✅ handoff.py — poll raw_documents,          extract rows, produce records
   update extractor_status               ✅ config.yaml
✅ kafka.py — CloudEvents producer        ✅ layouts/ (future: layout signature definitions)
✅ logging.py — structlog
✅ metrics.py — RunSummary
```

### 1.2 Core interfaces

```python
# libs/chip_extractors/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterator


@dataclass(frozen=True)
class SourceRecord:
    """One extracted record at source-native granularity.

    All field values are exactly as they appear in the source document.
    No canonical mapping, no disease-code translation, no P-code resolution.
    That is the normalizer's job.
    """
    payload: dict
    record_key: str              # Kafka message key: "<district>|<disease>|<year>-W<week>"
    occurred_at: datetime | None  # Event time from the source, if known


@dataclass(frozen=True)
class ExtractionInput:
    """One row from ingestion.raw_documents passed to the extractor."""
    id: int
    source: str
    identity: str
    bronze_uri: str
    content_hash: str
    content_type: str
    original_filename: str
    source_uri: str
    connector_version: str
    retrieved_at: datetime
    file_size_bytes: int


class Extractor(ABC):
    """Format-aware adapter. Reads a bronze artifact, extracts structured records.

    The extractor understands document STRUCTURE (where the tables are, how
    they're laid out). It does NOT understand data SEMANTICS (what disease
    labels mean, which P-code a district name maps to).
    """

    name: str
    extractor_version: str
    kafka_topic: str
    input_content_type: str       # "text/markdown", "application/json", etc.
    input_sources: list[str]      # which sources this extractor handles

    @abstractmethod
    def extract(
        self,
        inp: ExtractionInput,
        content: bytes,
        ctx: "ExtractorContext",
    ) -> Iterator[SourceRecord]:
        """Parse the bronze artifact and yield source-native records.

        Args:
            inp: Metadata about the document being extracted.
            content: The raw bytes from bronze (already fetched by the SDK).
            ctx: Injected services (Kafka producer, structlog, etc.).

        Yields:
            SourceRecord: One record per extracted data point. Yields nothing
            if the document contains no extractable data of this type.
        """
```

### 1.3 The SDK runner

The SDK owns the algorithm. The extractor fills in `extract()`. This is the same Template Method pattern as the connector SDK.

```python
# libs/chip_extractors/runner.py
from libs.chip_extractors.base import Extractor, ExtractionInput, RunSummary
from libs.chip_extractors.kafka import KafkaEnvelope


def run_extractor(ext: Extractor, ctx: "ExtractorContext") -> RunSummary:
    """
    The algorithm every extractor follows. Extractors never write this — they
    write extract().
    """
    summary = RunSummary(source=ext.name)
    ctx.log.info("extractor.run.started", extractor=ext.name)

    # ── Poll raw_documents for pending items ──────────────────────────
    pending = ctx.handoff.poll_pending(
        extractor_name=ext.name,
        sources=ext.input_sources,
        content_type=ext.input_content_type,
    )
    ctx.log.info("extractor.pending_items", count=len(pending))

    for inp in pending:
        ctx.log.debug("extractor.item.started", identity=inp.identity)

        # ── Mark as extracting ────────────────────────────────────────
        ctx.handoff.update_status(inp.id, ext.name, "extracting")

        try:
            # ── Fetch content from bronze ────────────────────────────
            content = ctx.bronze.get(inp.bronze_uri)
            summary.documents_read += 1

            # ── Extract records ──────────────────────────────────────
            records_produced = 0
            for rec in ext.extract(inp, content, ctx):
                envelope = KafkaEnvelope(
                    topic=ext.kafka_topic,
                    key=rec.record_key,
                    payload=rec.payload,
                    provenance={
                        "source": inp.source,
                        "connector_version": inp.connector_version,
                        "extractor_version": ext.extractor_version,
                        "bronze_uri": inp.bronze_uri,
                        "content_hash": inp.content_hash,
                        "source_uri": inp.source_uri,
                        "identity": inp.identity,
                        "retrieved_at": inp.retrieved_at.isoformat(),
                    },
                    occurred_at=rec.occurred_at,
                    epiweek_hint=inp.identity,  # e.g. "idsr:2025:W01"
                )
                ctx.kafka.produce(envelope)
                records_produced += 1

            # ── Mark as extracted ─────────────────────────────────────
            ctx.handoff.update_status(
                inp.id, ext.name, "extracted",
                metadata={"records_produced": records_produced},
            )
            summary.records_produced += records_produced
            summary.documents_extracted += 1

        except Exception as e:
            ctx.log.error("extractor.item.failed",
                          identity=inp.identity, error=str(e))
            ctx.handoff.update_status(
                inp.id, ext.name, "failed",
                error_message=str(e),
            )
            summary.errors += 1
            continue

    # ── Flush Kafka producer ─────────────────────────────────────────
    ctx.kafka.flush()

    ctx.metrics.emit(summary)
    ctx.log.info("extractor.run.completed", extractor=ext.name,
                 **summary.__dict__)
    return summary
```

### 1.4 Kafka message envelope (ADR-007 compliant)

Every record produced by the extractor uses the CloudEvents 1.0 envelope as specified in ADR-007:

```json
{
  "specversion": "1.0",
  "id": "01J9Z6K3M2T4V8QW2R7X",
  "source": "/chip/extractors/nih_idsr",
  "type": "pk.chip.health.disease_case_report.v1",
  "time": "2026-07-15T10:30:45Z",
  "datacontenttype": "application/json",
  "chip_epiweek": 202501,
  "chip_pcode": null,
  "chip_traceid": "a1b2c3d4",
  "data": {
    "payload": {
      "district": "Badin",
      "disease": "Malaria",
      "cases": 1261,
      "week": 1,
      "year": 2025
    },
    "provenance": {
      "source": "nih_idsr",
      "connector_version": "1.0.0",
      "extractor_version": "1.0.0",
      "bronze_uri": "s3://chip-bronze/nih_idsr/idsr:2025:W01/sha256-abc.../Week-01-2025.md",
      "content_hash": "sha256:abc123...",
      "source_uri": "Data_sources_1/NIH/MD/Week-01-2025.md",
      "identity": "idsr:2025:W01",
      "retrieved_at": "2026-07-15T10:30:00Z"
    }
  }
}
```

**Key properties:**
- `data.payload` contains source-native field names exactly as they appear in the MD table headers
- `data.provenance` carries the full chain: connector → extractor → bronze artifact
- `chip_epiweek` is duplicated at the envelope level so downstream consumers can route without deserializing `data`
- `chip_pcode` is `null` — the normalizer fills it in
- The `specversion`, `id`, `type`, `source`, `time` fields are CloudEvents standard; `chip_*` are CHIP extensions

### 1.5 Kafka topic naming (ADR-007)

```
chip.health.nih_idsr.disease_case_report.v1
chip.health.pitb_dss.disease_case_report.v1
chip.health.ajk_idsrs.disease_case_report.v1
chip.health.dhis_punjab_weekly.disease_case_report.v1
```

Each extractor produces to exactly one topic. The normalizer subscribes to all four.

### 1.6 Logging & metrics

Identical pattern to connectors: `structlog` JSON logs, `RunSummary` emits per-run counts.

```python
@dataclass
class RunSummary:
    extractor: str
    documents_read: int = 0
    documents_extracted: int = 0
    records_produced: int = 0
    errors: int = 0
    duration_ms: int = 0
```

---

## 2. Connector → Extractor → Normalizer Handoff

### 2.1 The `extractor_status` tracking table

Each extractor independently tracks which documents it has processed. A single document can be `extracted` by the disease table extractor and `pending` for the prose extractor.

```sql
CREATE TABLE ingestion.extractor_status (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_document_id BIGINT NOT NULL REFERENCES ingestion.raw_documents(id),
    extractor_name  TEXT NOT NULL,           -- "nih_idsr_disease_tables"
    status          TEXT NOT NULL DEFAULT 'pending',
                    -- pending | extracting | extracted | failed
    records_produced INT,                    -- set when status = 'extracted'
    error_message   TEXT,                    -- set when status = 'failed'
    error_at        TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (raw_document_id, extractor_name)
);

CREATE INDEX idx_extractor_status_pending
    ON ingestion.extractor_status (extractor_name, status)
    WHERE status = 'pending';
```

The `raw_documents.status` column is removed. All extraction state lives here.

**Polling query (per extractor):**
```sql
SELECT rd.*
FROM ingestion.raw_documents rd
JOIN ingestion.extractor_status es
    ON es.raw_document_id = rd.id
WHERE es.extractor_name = 'nih_idsr_disease_tables'
  AND es.status = 'pending'
  AND rd.source IN ('nih_idsr')
  AND rd.content_type = 'text/markdown'
ORDER BY rd.created_at;
```

**Status transitions:**
```
pending ──→ extracting ──→ extracted
   │                          │
   └──────── failed ──────────┘
```

- `pending → extracting`: set when `run_extractor()` picks up the item
- `extracting → extracted`: set after successful Kafka produce + flush
- `extracting → failed`: set on any exception during `extract()` or Kafka produce
- `failed → pending`: manually reset by an operator to trigger re-extraction

### 2.2 Seeding `extractor_status` rows

When a connector inserts a row into `raw_documents`, a Postgres trigger or the connector's `handoff.signal()` method also inserts one row per registered extractor for that source:

```sql
-- Trigger: after INSERT on raw_documents
INSERT INTO ingestion.extractor_status (raw_document_id, extractor_name)
SELECT NEW.id, er.extractor_name
FROM ingestion.extractor_registry er
WHERE er.source = NEW.source;
```

`extractor_registry` is a small lookup table:
```sql
CREATE TABLE ingestion.extractor_registry (
    source          TEXT NOT NULL,
    extractor_name  TEXT NOT NULL,
    PRIMARY KEY (source, extractor_name)
);

-- Seed data for prototype:
INSERT INTO ingestion.extractor_registry VALUES
    ('nih_idsr',            'nih_idsr_disease_tables'),
    ('pitb_dss',            'pitb_dss_disease_tables'),
    ('ajk_idsrs',           'ajk_idsrs_disease_tables'),
    ('dhis_punjab_weekly',  'dhis_punjab_disease_tables');
```

When a new extractor is added in the future, add a row to `extractor_registry` and backfill `extractor_status` for existing documents:

```sql
INSERT INTO ingestion.extractor_status (raw_document_id, extractor_name)
SELECT rd.id, 'nih_idsr_prose'
FROM ingestion.raw_documents rd
WHERE rd.source = 'nih_idsr';
```

---

## 3. Per-Source Extractor Specifications

### 3.1 NIH IDSR — Disease Table Extractor

#### 3.1.1 Extractor identity

| Attribute | Value |
|---|---|
| Extractor name | `nih_idsr_disease_tables` |
| Extractor version | `1.0.0` |
| Input content type | `text/markdown` |
| Input sources | `nih_idsr` |
| Kafka topic | `chip.health.nih_idsr.disease_case_report.v1` |

#### 3.1.2 Document structure knowledge

The NIH bulletin has 5+ data tables embedded in markdown with HTML `<table>` tags. The extractor must locate the correct tables among the narrative prose, figures, compliance tables, and editorial content.

**Tables to extract:**

| Table | Content | How to find it |
|---|---|---|
| **Table 1 — Province Summary** | Disease rows × province columns (+ Total) | Look for a table immediately after a heading containing "Overview" or near the start of the document. The first data table with multiple province name columns. |
| **Table 2 — Sindh District Detail** | District rows × disease columns (or vice versa, era-dependent) | Look for a table after text containing "Sindh" AND district names like "Badin", "Dadu", "Ghotki". |
| **Table 3 — Balochistan District Detail** | District rows × disease columns | Look for a table after text containing "Balochistan" AND district names like "Quetta", "Gwadar", "Pishin". |
| **Table 4 — KP District Detail** | District rows × disease columns | Look for a table after text containing "KP" or "Khyber Pakhtunkhwa" AND district names like "Peshawar", "Swat", "Abbottabad". |

**Tables to IGNORE:**
- Compliance tables (columns: Province, District, Reporting Sites, Compliance %)
- Public Health Laboratories table (2024+: columns contain "Total Test", "Total Positive")
- Trend figure data (no `<table>` tag — these are images or narrative text)
- IDSR Participating Districts table (same structure as compliance tables)
- Any table whose headers contain "Percentage", "Compliance", "Report", "Laboratory", "Test", "Positive"

#### 3.1.3 Era detection

The extractor must detect the table orientation before extracting rows.

**Era 1 (2021–2022): Transposed orientation**
```
Diseases            | Ghotki | Hyderabad | Karachi East | ... | Total
ILI                 | 424    | 3,130     | 1            | ... | 4,271
```

Detection: the first column header contains "Disease" (singular/plural, case-insensitive) AND the remaining columns are district names or province names.

**Era 2 (2023–2026): Pivoted orientation**
```
DISTRICTS  | Malaria | AD (Non-Cholera) | ILI | ... | Total
Badin      | 1,668   | 1,902            | 544 | ... | 4,508
```

Detection: the first column header contains "District" (case-insensitive) AND the remaining columns are disease names.

**Heuristic for auto-detection:**

```python
def detect_orientation(headers: list[str]) -> str:
    first_header = headers[0].strip().lower()
    if "disease" in first_header:
        return "transposed"   # diseases = rows, districts = cols
    if "district" in first_header:
        return "pivoted"      # districts = rows, diseases = cols
    # Fallback: count how many remaining headers look like disease names
    # vs. district names. The majority wins.
    disease_hits = sum(1 for h in headers[1:] if h in KNOWN_DISEASE_LABELS)
    district_hits = sum(1 for h in headers[1:] if h in KNOWN_DISTRICT_NAMES)
    return "pivoted" if disease_hits >= district_hits else "transposed"
```

`KNOWN_DISEASE_LABELS` and `KNOWN_DISTRICT_NAMES` are seeded from the Layer 1 catalog's reference tables (§6.1–6.2).

#### 3.1.4 Data extraction

For **Era 2 (pivoted)**:

```python
# headers: ["DISTRICTS", "Malaria", "AD (Non-Cholera)", "ILI", ..., "Total"]
# rows: first cell = district name, remaining cells = case counts

for row in rows:
    district = row[0]
    if district in ("", "DISTRICTS"):
        continue  # skip header rows
    if district == "Total":
        # Emit Total row as tagged record for normalizer reconciliation
        for i in range(1, len(headers) - 1):
            disease = headers[i]
            if disease == "Total":
                continue
            cases_str = row[i]
            if cases_str in ("", "NR", None):
                continue
            try:
                cases = int(cases_str.replace(",", ""))
            except ValueError:
                continue
            yield SourceRecord(
                payload={..., "row_type": "total"},
                record_key=f"TOTAL|{disease}|{year}-W{week:02d}",
            )
        continue
    for i in range(1, len(headers) - 1):  # skip last column ("Total")
        disease = headers[i]
        cases_str = row[i]
        if cases_str in ("", "NR", None):
            continue
        try:
            cases = int(cases_str.replace(",", ""))
        except ValueError:
            continue
        yield SourceRecord(
            payload={
                "district": district,
                "disease": disease,
                "cases": cases,
                "week": week,
                "year": year,
                "table": "sindh_district",
                "source": "nih_idsr",
                "row_type": "data",
            },
            record_key=f"{district}|{disease}|{year}-W{week:02d}",
            occurred_at=None,
        )
```

For **Era 1 (transposed)**, the iteration is inverted:
```python
# headers: ["Diseases", "Ghotki", "Hyderabad", ..., "Total"]
# rows: first cell = disease name, remaining cells = district case counts

for row in rows:
    disease = row[0]
    for i in range(1, len(headers) - 1):
        district = headers[i]
        cases_str = row[i]
        # ... same extraction logic ...
```

#### 3.1.5 The province summary table (Table 1)

The province summary table is functionally identical to the district tables but at province granularity. Its records carry `table: "province_summary"` in the payload. The normalizer can distinguish these and apply different handling (per L1-03, province-level records are produced with a provenance flag).

For the **AJK, GB, ICT, and Punjab** columns in Table 1 — these are province-level records with no district breakdown. The extractor produces them anyway with the province name as the "district" value and `table: "province_summary"`. The normalizer decides what to do with them.

#### 3.1.6 Edge cases

| Case | Behavior |
|---|---|
| Unknown table orientation (neither "disease" nor "district" in headers) | Log WARN, extractor fails for this document. `extractor_status = failed`. Human triages. |
| Cell value is "NR" | Skip — not a numeric case count. |
| Cell value is empty or whitespace | Skip. |
| Cell value contains commas ("1,668") | Strip commas before int parsing. |
| Row is a total row (label is "Total") | Emit as separate record with `row_type: "total"` in payload. The normalizer uses these for reconciliation. |
| Table has mismatched row/column counts | Log WARN, extract what we can, skip the rest. Don't fail the whole document. |
| Multiple tables match the same province (duplicate detection) | Extract both. The normalizer's UPSERT handles duplicates via record_key. |
| 2021–2022 bulletins: Punjab column absent from Table 1 | No Punjab records produced. This is correct — Punjab didn't report to IDSR then. |
| 2023 bulletins: Punjab column present but all "NR" | No Punjab records produced (all "NR" → all skipped). Correct. |
| 2024+ bulletins: Punjab has real data in Table 1 but no district table | Province-level records produced for Punjab. Normalizer tags them. |

#### 3.1.7 Config

```yaml
# extractors/nih_idsr_disease_tables/config.yaml
extractor_name: nih_idsr_disease_tables
extractor_version: "1.0.0"
input_content_type: text/markdown
input_sources:
  - nih_idsr
kafka_topic: chip.health.nih_idsr.disease_case_report.v1

# Table-finding strategy
tables:
  province_summary:
    location: "first data table in document"
    header_hint: "disease"
  sindh_district:
    location: "after text containing 'Sindh'"
    header_hint: "district"
    known_districts: ["Badin", "Dadu", "Ghotki", "Hyderabad", "Jakobabad",
                      "Jamshoro", "Kamber", "Karachi", "Kashmore", "Khairpur",
                      "Larkana", "Matiari", "Mirpur", "Naushahro", "Sanghar",
                      "Shaheed Benazirabad", "Shikarpur", "Sujawal", "Sukkur",
                      "Tando", "Tharparkar", "Thatta", "Umerkot"]
  balochistan_district:
    location: "after text containing 'Balochistan'"
    known_districts: ["Quetta", "Gwadar", "Pishin", "Killa", "Loralai", "Zhob",
                      "Khuzdar", "Chagai", "Sibi", "Kalat", "Mastung", "Nushki",
                      "Panjgur", "Washuk", "Awaran", "Kharan", "Lasbela",
                      "Jaffarabad", "Naseerabad", "Jhal Magsi", "Kachhi",
                      "Dera Bugti", "Kohlu", "Barkhan", "Musakhel", "Sherani",
                      "Ziarat", "Harnai", "Sohbatpur", "Duki", "Chaman", "Surab"]
  kp_district:
    location: "after text containing 'KP' or 'Khyber'"
    known_districts: ["Peshawar", "Swat", "Abbottabad", "Mardan", "Nowshera",
                      "Charsadda", "Mansehra", "Kohat", "D.I. Khan", "Bannu",
                      "Haripur", "Swabi", "Buner", "Battagram", "Shangla",
                      "Malakand", "Hangu", "Karak", "Lakki Marwat",
                      "Lower Dir", "Upper Dir", "Chitral", "Kohistan",
                      "Torghar", "Tank", "Mohmand", "Bajaur", "Khyber",
                      "Orakzai", "Kurram", "North Waziristan", "South Waziristan"]

era_detection:
  strategy: auto
  era1_indicators: ["first column header contains 'disease'"]
  era2_indicators: ["first column header contains 'district'"]
  known_disease_labels_seed: docs/architecture_v1/zoomed_in_layer/layer1/01-data-sources-complete-catalog.md#6.2
  known_district_names_seed: docs/architecture_v1/zoomed_in_layer/layer1/01-data-sources-complete-catalog.md#6.1

skip_tables:
  header_indicators:
    - "percentage"
    - "compliance"
    - "reporting sites"
    - "laboratory"
    - "total test"
    - "total positive"

schedule:
  cron: "0 7 * * 2"           # Runs after connector (Tuesdays 07:00 PKT)
  timezone: Asia/Karachi
```

---

### 3.2 PITB-DSS — Disease Table Extractor

#### 3.2.1 Extractor identity

| Attribute | Value |
|---|---|
| Extractor name | `pitb_dss_disease_tables` |
| Extractor version | `1.0.0` |
| Input content type | `text/markdown` |
| Input sources | `pitb_dss` |
| Kafka topic | `chip.health.pitb_dss.disease_case_report.v1` |

#### 3.2.2 Document structure knowledge

PITB-DSS bulletins have a simpler structure than NIH. The main disease table is the communicable disease situation section. All years (2015–2018) use a consistent transposed orientation: diseases as rows, districts as columns. 36 Punjab districts across all columns.

**Table to extract:**
- The communicable disease situation table — rows = diseases, columns = districts
- Found after the masthead and disease summary boxes
- Columns are Punjab district names (e.g., "Lahore", "Faisalabad", "DG Khan")

**Key differences from NIH:**
- Only ONE main table (no per-province split)
- Always transposed (diseases as rows) — no era detection needed
- ALL CAPS or abbreviated district names — the normalizer handles this

#### 3.2.3 Extraction logic

```python
def extract(self, inp, content, ctx):
    doc = markdown_to_soup(content)
    tables = find_all_tables(doc)

    for table in tables:
        headers = extract_headers(table)
        if not any(d in headers for d in PUNJAB_DISTRICTS):
            continue  # not the disease table

        for row in table_rows:
            disease = row[0]
            for i in range(1, len(headers)):
                district = headers[i]
                cases_str = row[i]
                # ... extract integer, skip NR/empty ...
                yield SourceRecord(...)
```

#### 3.2.4 Config

```yaml
# extractors/pitb_dss_disease_tables/config.yaml
extractor_name: pitb_dss_disease_tables
extractor_version: "1.0.0"
input_content_type: text/markdown
input_sources:
  - pitb_dss
kafka_topic: chip.health.pitb_dss.disease_case_report.v1

tables:
  disease_matrix:
    location: "after communicable disease heading"
    orientation: transposed          # diseases = rows, districts = cols
    known_districts:                 # 36 Punjab districts
      - "Lahore", "Faisalabad", "Rawalpindi", "Multan", "Gujranwala",
        "Sialkot", "Sargodha", "Bahawalpur", "DG Khan", "Rahim Yar Khan",
        "Sahiwal", "Jhang", "Kasur", "Okara", "Sheikhupura", "Vehari",
        "Muzaffargarh", "Khanewal", "Pakpattan", "Bahawalnagar", "Attock",
        "Jhelum", "Mianwali", "Khushab", "Layyah", "Bhakkar", "Chakwal",
        "Toba Tek Singh", "Hafizabad", "Mandi Bahauddin", "Narowal",
        "Nankana Sahib", "Chiniot", "Gujrat", "Lodhran", "Rajanpur"

schedule:
  cron: "0 7 * * 3"
  timezone: Asia/Karachi
```

---

### 3.3 AJK IDSRS — Disease Table Extractor

#### 3.3.1 Extractor identity

| Attribute | Value |
|---|---|
| Extractor name | `ajk_idsrs_disease_tables` |
| Extractor version | `1.0.0` |
| Input content type | `text/markdown` |
| Input sources | `ajk_idsrs` |
| Kafka topic | `chip.health.ajk_idsrs.disease_case_report.v1` |

#### 3.3.2 Document structure knowledge

AJK bulletins have the richest structure of all MD sources — 3 distinct data tables per bulletin plus weekly comparison tables.

**Tables to extract:**

| Table | Content | Orientation |
|---|---|---|
| **Overall Suspected Cases & Deaths** | Disease rows × Cases + Deaths columns | Transposed: disease=row, value=cell |
| **District Wise Detail** | Disease rows × 10 district columns | Transposed: disease=row, district=col |
| **Weekly Comparisons** | 3-week trend per disease category | Transposed: disease=row, week columns |

**Key differences from NIH/PITB:**
- Deaths are tracked explicitly (separate column)
- District columns use abbreviations: `MZD`, `JV`
- Weekly comparison tables provide 3-week trends — extract for trend validation

#### 3.3.3 Extraction logic

For the deaths column in the overall table:
```python
yield SourceRecord(
    payload={
        "district": None,           # AJK province-level
        "disease": "Acute Diarrhea (Non-Cholera)",
        "cases": 2146,
        "deaths": 0,
        "table": "overall_cases_and_deaths",
        "week": 18,
        "year": 2026,
        "source": "ajk_idsrs",
    },
    record_key=f"ajk|Acute Diarrhea (Non-Cholera)|2026-W18",
    occurred_at=None,
)
```

#### 3.3.4 Config

```yaml
# extractors/ajk_idsrs_disease_tables/config.yaml
extractor_name: ajk_idsrs_disease_tables
extractor_version: "1.0.0"
input_content_type: text/markdown
input_sources:
  - ajk_idsrs
kafka_topic: chip.health.ajk_idsrs.disease_case_report.v1

tables:
  overall_cases:
    location: "before district detail table"
    has_deaths_column: true
    orientation: transposed
  district_detail:
    location: "after heading 'District Wise Detail'"
    orientation: transposed          # diseases = rows, districts = cols
    known_districts:
      - "MZD", "JV", "Neelum", "Poonch", "Bagh", "Haveli",
        "Sudhnoti", "Mirpur", "Bhimber", "Kotli"
  weekly_comparisons:
    location: "after heading 'Weekly Comparisons'"
    include: true                    # extract 3-week trend for validation

schedule:
  cron: "0 7 * * 2"
  timezone: Asia/Karachi
```

---

### 3.4 DHIS Punjab — Disease Table Extractor

#### 3.4.1 Extractor identity

| Attribute | Value |
|---|---|
| Extractor name | `dhis_punjab_disease_tables` |
| Extractor version | `1.0.0` |
| Input content type | `text/markdown` |
| Input sources | `dhis_punjab_weekly` |
| Kafka topic | `chip.health.dhis_punjab_weekly.disease_case_report.v1` |

#### 3.4.2 Document structure knowledge

The DHIS-II era (2022) weekly reports have 11+ data tables. Two are relevant for disease extraction:

**Tables to extract:**

**Table 1 — Prone Epidemic Diseases:** 31 district rows × 12 disease columns. ALL CAPS district names. Diseases: ILI, AFP, Typhoid, HIV/AIDS, Measles, Meningitis, NNT, CCHF, Dengue, Diphtheria, Pertussis, Chicken Pox.

**Table 2 — Suspected OPD Disease-wise New Cases:** ~55-68 diseases, province-level aggregate. Diseases grouped into 15 categories (Respiratory, Communicable, Skin, Cardiovascular, etc.). Columns: sr., Disease Category, Disease Name, Count.

**How to find Table 1:**
- Look for a table whose headers include at least 6 of the 12 epidemic disease names
- AND whose first column contains ALL CAPS Punjab district names

**How to find Table 2:**
- Look for a table whose headers include `Disease Category` or `Disease Name`
- AND whose rows contain disease names across 15+ medical categories
- Usually appears after the OPD attendance section

**Tables to IGNORE:**
- OPD attendance tables (headers: Age Group, Male, Female)
- Delivery/ANC/FP tables (headers: BHU, RHC, THQ, DHQ, THOS)
- Compliance tables (headers: Daily IPD, Daily OPD, Daily RMNCH)
- Indoor admission/death tables
- Trend analysis tables (daily dates)

#### 3.4.3 Config

```yaml
# extractors/dhis_punjab_disease_tables/config.yaml
extractor_name: dhis_punjab_disease_tables
extractor_version: "1.0.0"
input_content_type: text/markdown
input_sources:
  - dhis_punjab_weekly
kafka_topic: chip.health.dhis_punjab_weekly.disease_case_report.v1

tables:
  epidemic_diseases:
    location: "after heading 'Prone Epidemic Diseases'"
    orientation: pivoted              # districts = rows, diseases = cols
    grain: district                   # district-level data
    known_diseases:
      - "ILI", "AFP", "Typhoid", "HIV/AIDS", "Measles", "Meningitis",
        "NNT", "CCHF", "Dengue", "Diphtheria", "Pertussis", "Chicken Pox"
    detection_rule: ">= 6 of 12 disease headers present"
    known_districts:                  # 31 districts (DHIS-II era, ALL CAPS)
      - "ATTOCK", "BAHAWALNAGAR", "BAHAWALPUR", "BHAKKAR", "CHAKWAL",
        "CHINIOT", "DG KHAN", "FAISALABAD", "GUJRANWALA", "GUJRAT",
        "HAFIZABAD", "JHANG", "JHELUM", "KASUR", "KHANEWAL", "KHUSHAB",
        "LAHORE", "LAYYAH", "LODHRAN", "MANDI BAHAUDDIN", "MIANWALI",
        "MULTAN", "MUZAFFARGARH", "NANKANA SAHIB", "NAROWAL", "OKARA",
        "PAKPATTAN", "RAHIM YAR KHAN", "RAJANPUR", "RAWALPINDI", "SAHIWAL",
        "SARGODHA", "SHEIKHUPURA", "SIALKOT", "TOBA TEK SINGH", "VEHARI"
  opd_disease_table:
    location: "after OPD attendance tables, before Top 10 lists"
    orientation: key_value             # Disease Category + Disease Name → Count
    grain: province                    # province-level aggregate
    known_category_headers: ["Disease Category", "Disease Name"]
    known_categories:
      - "Respiratory", "Communicable", "Skin", "Cardiovascular",
        "Psychiatric", "Eye/ENT", "Gastrointestinal", "Vaccine Preventable",
        "Cancer", "Oral", "Neurological", "Injuries", "Endocrine", "STI"
    detection_rule: ">= 3 of 15 category headers OR column contains 'Disease Category'"

schedule:
  cron: "0 7 * * 3"
  timezone: Asia/Karachi
```

---

## 4. Format-Change Resilience

### 4.1 The extraction failure path

When a document changes format, the extractor fails. The connector is unaffected — it keeps archiving. The normalizer is unaffected — it never sees the bad records.

```
Format change (e.g., NIH adds a new table layout in 2027)
    │
    ▼
Extractor: extract() encounters unrecognized table structure
    │
    ├── extractor fails for this document
    ├── extractor_status = "failed"
    ├── error_message = "Unknown layout signature: headers=['DISTRICTS','NEW_COLUMN',...]"
    ├── No partial records produced to Kafka
    └── Operator alerted
          │
          ▼
    Fix: update extractor table-finding logic or add new era config
    Bump extractor_version
    Reset extractor_status for affected documents to "pending"
    Re-run extractor → successful extraction
```

### 4.2 What changes where

| Change | Connector | Extractor | Normalizer |
|---|---|---|---|
| File naming convention | Update config regex | None | None |
| Document structure / table layout | None | Update extractor logic, bump version, replay from bronze | None |
| New disease label in table headers | None | None (emits as-is) | Add to disease crosswalk |
| New district name | None | None (emits as-is) | Add to location_alias |
| Column ordering changes | None | None (headers are read dynamically) | None |
| Source URL changes (future) | Update config | None | None |

---

## 5. Monorepo Layout

```
libs/
  chip_extractors/                       # SDK (shared, written once)
    __init__.py
    base.py                              # Extractor ABC, SourceRecord, ExtractionInput
    runner.py                            # run_extractor() — the algorithm
    handoff.py                           # poll_pending(), update_status()
    kafka.py                             # KafkaEnvelope, CloudEvents producer
    logging.py                           # structlog
    metrics.py                           # RunSummary

extractors/                              # One package per source (thin)
  nih_idsr_disease_tables/
    __init__.py
    extractor.py                         # NihIdsrDiseaseTableExtractor(Extractor)
    config.yaml
  pitb_dss_disease_tables/
    __init__.py
    extractor.py                         # PitbDssDiseaseTableExtractor(Extractor)
    config.yaml
  ajk_idsrs_disease_tables/
    __init__.py
    extractor.py                         # AjkIdsrsDiseaseTableExtractor(Extractor)
    config.yaml
  dhis_punjab_disease_tables/
    __init__.py
    extractor.py                         # DhisPunjabDiseaseTableExtractor(Extractor)
    config.yaml
  README.md                              # "How to add an extractor" guide

pipelines/
  extraction/
    assets.py                            # Dagster SDAs per extractor
    resources.py                         # Dagster resource → ExtractorContext
    schedules.py                         # Cron schedules (after connector schedules)

infra/
  docker/
    Dockerfile.extractor                 # Python 3.12 + kafka-python + structlog + dagster
  compose/
    docker-compose.extraction.yaml       # Kafka broker, extractor containers
```

---

## 6. Dagster Integration

### 6.1 Assets per extractor

```python
# pipelines/extraction/assets.py
from dagster import asset, AssetExecutionContext

@asset(
    group_name="extraction",
    kinds={"kafka", "minio", "postgres"},
    deps=["raw_nih_idsr"],           # runs after the connector asset
)
def extracted_nih_idsr(context: AssetExecutionContext, extractor_ctx: ExtractorContext):
    from extractors.nih_idsr_disease_tables.extractor import NihIdsrDiseaseTableExtractor
    summary = run_extractor(NihIdsrDiseaseTableExtractor(), extractor_ctx)
    context.add_output_metadata({
        "documents_read": summary.documents_read,
        "documents_extracted": summary.documents_extracted,
        "records_produced": summary.records_produced,
        "errors": summary.errors,
    })
```

### 6.2 Schedules

Each extractor runs after its corresponding connector:

```
Connector schedule        Extractor schedule
══════════════════        ══════════════════
nih_idsr: Tue 06:00  →   nih_idsr_disease_tables: Tue 07:00
pitb_dss: Wed 06:00  →   pitb_dss_disease_tables: Wed 07:00
ajk_idsrs: Tue 06:00 →   ajk_idsrs_disease_tables: Tue 07:00
dhis_punjab: Wed 06:00 → dhis_punjab_disease_tables: Wed 07:00
```

For the prototype backfill, all extractors can be triggered as a single Dagster backfill job after all connectors have completed.

---

## 7. Open Questions & Design Decisions

### 7.1 Resolved

| ID | Decision | Rationale |
|---|---|---|
| **L3-01** | One extractor per source for MD disease tables. | Each source's table structure is unique — table-finding logic, era detection, and orientation handling differ. A shared extractor would be a switch statement over 4+ strategies. |
| **L3-02** | Extractor → normalizer handoff uses Kafka (ADR-007 topics). | ADR-002 specifies Kafka as the backbone. Kafka decouples the extractor from the normalizer and enables replay. |
| **L3-03** | Extractor emits source-native labels. Normalizer maps to canonical codes. | Separation of concerns: extractor knows document structure, normalizer knows data semantics. |
| **L3-04** | Totals reconciliation lives in the normalizer, NOT the extractor. | Reconciliation needs all records from a document collected and grouped by table type. The extractor emits records one at a time and can't do cross-table arithmetic. |
| **L3-05** | `extractor_status` is a separate table tracking per-`(document, extractor)` state. | Multiple extractors will process the same document in the future (disease tables, prose, compliance). A single status column can't represent this. |
| **L3-06** | `extractor_registry` seeds `extractor_status` rows automatically when a connector inserts a `raw_documents` row. | Postgres trigger or connector handoff logic ensures no document is missed. Adding a new extractor means inserting one row into `extractor_registry` and backfilling status rows. |
| **L3-10** | Era detection failure → `extractor_status = failed`, human triages. | The prototype has ~400 documents. A human reviewing 3–5 era-detection failures is feasible. No secondary heuristic needed at this scale. |
| **L3-11** | Extractors **emit the Total column** as a separate record tagged with `row_type: "total"`. | The normalizer needs printed totals for reconciliation (sum of districts must equal printed total). If the extractor skips totals, the normalizer must re-read bronze to get them. Emitting them as tagged records is cheaper and keeps the normalizer's data source as Kafka only. |
| **L3-12** | DHIS extractor **also extracts the OPD disease table** (68 diseases, province-level). | The DHIS-II era OPD table is structured data with disease names and counts in a consistent format. Even though it's province-level, it adds disease coverage for 56 diseases not in the 12-disease epidemic table. The normalizer can handle province-level records separately. |

### 7.2 Deferred to future

| ID | Question | Why deferred |
|---|---|---|
| **L3-07** | When the JSON API extractor is built (Open-Meteo weather), does it need the same extractor SDK or a different one? | The JSON API extractor has a fundamentally different pattern — API pagination, structured schema, no table parsing. It may need a different base class. |
| **L3-08** | Should the extractor produce one Kafka message per record or batch them? | For prototype volume (~10K records), per-record is fine. Batch if throughput becomes an issue. |
| **L3-09** | Should the extractor validate that the identity's year/week matches the year/week encoded in the document title? | The connector already derived identity from the file. Cross-validating against document content is defense-in-depth — good for production, overkill for prototype. |

### 7.3 Open

*None remaining at Layer 3.*

---

## Appendix A: Kafka Topic Specifications

| Topic | Producer | Consumer | Key | Payload schema |
|---|---|---|---|---|
| `chip.health.nih_idsr.disease_case_report.v1` | `nih_idsr_disease_tables` | Disease Normalizer | `district\|disease\|year-Wweek` | `{ district, disease, cases, week, year, table, source }` |
| `chip.health.pitb_dss.disease_case_report.v1` | `pitb_dss_disease_tables` | Disease Normalizer | `district\|disease\|year-Wweek` | `{ district, disease, cases, week, year, table, source }` |
| `chip.health.ajk_idsrs.disease_case_report.v1` | `ajk_idsrs_disease_tables` | Disease Normalizer | `district\|disease\|year-Wweek` | `{ district, disease, cases, deaths, week, year, table, source }` |
| `chip.health.dhis_punjab_weekly.disease_case_report.v1` | `dhis_punjab_disease_tables` | Disease Normalizer | `district\|disease\|year-Wweek` | `{ district, disease, cases, week, year, table, source }` |

---

## Appendix B: Document History

| Date | Author | Change |
|---|---|---|
| 2026-07-15 | Architecture team | Initial Layer 3 zoomed-in design — MD table extractors for 4 prototype sources |
| 2026-07-15 | Architecture team | Resolved L3-10, L3-11, L3-12. Extractors emit total rows. DHIS OPD table included. |
