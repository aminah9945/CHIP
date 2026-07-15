# CHIP — Layer 2: Data Collection Connectors (Thin Connector Model)

**Zoomed-in layer:** Layer 2 — Collection / Connectors
**Parent document:** `ARCHITECTURE-DIAGRAM-GUIDE.md` Block 2
**Status:** Design (prototype phase — static local data, thin connector model)
**Audience:** MS/PhD implementers building and extending connectors
**Last updated:** 2026-07-15

> This document defines the connector subsystem at microscopic detail: the shared SDK framework, the per-source connector specifications, the connector-to-extractor handoff, format-change resilience, and the monorepo structure. It supersedes the connector portions of `02-ingestion-connectors.md` for the prototype phase while remaining aligned with the ADRs that govern topology, storage, and the canonical grain.

---

## 0. Architecture Decision: Thin Connector Model

### 0.1 Why connectors are thinner than the original 02 spec

The upstream `02-ingestion-connectors.md` §1.2 defines a six-stage lifecycle where the connector owns `parse` and `validate`. That design assumes the connector is the structure-extraction boundary: raw PDF/HTML in, validated disease records out.

The prototype data changes this assumption in two ways:

1. **The raw artifacts are markdown, not PDFs.** These MD files contain the full epidemiological bulletin — tables, narrative prose, outbreak investigations, compliance data, editorials. Treating them as "data tables to extract" discards the prose content that the knowledge graph, RAG, and cited-summary assistant will consume later.

2. **Multiple downstream consumers extract different things from the same document.** The analytics pipeline needs district × disease tables. The knowledge graph builder needs entity mentions from narrative sections. The RAG system needs the full text. All three read the same bronze artifact. If the connector extracts tables and discards the rest, the other consumers lose access to the original document without a second fetch.

**Decision:** The connector is a thin archival gateway. It discovers, fetches, and archives the complete raw document to bronze, then signals downstream that a new document is available. It does NOT parse tables, validate records, or produce structured `SourceRecord`s.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 2: CONNECTOR (this document)                                     │
│                                                                         │
│  discover → fetch → archive_raw → signal                                 │
│     │         │          │           │                                   │
│     │         │          │           └─ INSERT INTO ingestion.raw_documents
│     │         │          └─ SDK: MinIO bronze (content-addressed, WORM) │
│     │         └─ Read local MD file (prototype)                         │
│     └─ Scan Data_sources_1/ directories, derive stable identity keys    │
└────────────────────────────────────────│────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 3: DOCUMENT EXTRACTOR (see layer3/03-document-extractors.md)     │
│                                                                         │
│  Poll ingestion.extractor_status WHERE status = 'pending' →             │
│  Read bronze_uri from MinIO → Parse MD tables →                         │
│  Produce SourceRecords to Kafka                                         │
└────────────────────────────────────────│────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 4: NORMALIZER                                                    │
│                                                                         │
│  Map SourceRecords → canonical grain (district × epi-week × disease)    │
└─────────────────────────────────────────────────────────────────────────┘
```

### 0.2 Prototype vs. production handoff

| Dimension | Prototype (now) | Production (future) |
|---|---|---|
| Fetch source | Local filesystem (`Data_sources_1/`) | HTTP (PDF URLs from NIH, NDMA, etc.) |
| Connector→extractor handoff | Postgres `ingestion.raw_documents` table | Kafka `chip.<domain>.<source>.document_ingested.v1` |
| Archive content | Pre-parsed `.md` files | Original PDF, HTML, JSON payloads |
| Dedup granularity | Identity dedup (content hash in Postgres) | Identity + content dedup (3 layers per 02 spec) |
| Scheduling | One-time backfill (Dagster partitioned assets) | Cron schedules per source |

The connector SDK (`base.py`, `runner.py`, `bronze.py`) is written to support both modes through a `fetch_mode` config field. The connector author's `fetch()` implementation changes; the framework around it does not.

### 0.3 Design priorities

1. **Bronze is the system of record.** Every link in the provenance chain starts here.
2. **Add a source in a day.** A new connector is 4 methods + a config file. The SDK handles everything else.
3. **Format-change resilience.** A changed document layout quarantines downstream (in the extractor), not upstream (the connector keeps archiving).
4. **Correctness over throughput.** Volume is trivial (~400 documents total for prototype). We optimize for auditability.

---

## 1. Connector SDK (`libs/chip_connectors`)

### 1.1 SDK vs. connector — what goes where

The SDK is the shared infrastructure written once and reused by all connectors. The connector is the thin source-specific plugin.

```
SDK (libs/chip_connectors/)          CONNECTOR (connectors/nih_idsr/)
═══════════════════════════          ════════════════════════════════
✅ base.py — Connector ABC           ✅ connector.py — NihIdsrConnector(Connector)
✅ runner.py — run_connector()       │  ├─ discover() — scan directory
✅ bronze.py — MinIO archival        │  ├─ fetch()    — read .md file
✅ dedup.py — identity/watermarks    │  └─ derive_identity() — parse filename
✅ logging.py — structlog            ✅ config.yaml
✅ metrics.py — RunSummary           ✅ schemas.py — pydantic models for
✅ http.py — (stub for future)          the raw_documents row payload
```

**What a connector author NEVER writes:**
- MinIO put/get logic
- Content hashing
- Dedup checks against Postgres
- Watermark advancement
- Structured logging
- The `raw_documents` INSERT
- The `RunSummary` emission

**What a connector author ALWAYS writes:**
- `discover()` — where to find candidate files/URLs and how to derive stable identities
- `fetch()` — how to retrieve the bytes
- `derive_identity()` — how to compute the stable natural key from a filename/path/URL
- `config.yaml` — source metadata, schedule, fetch configuration

### 1.2 Core interfaces

```python
# libs/chip_connectors/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator
import hashlib


# ---- Value objects -------------------------------------------------------

@dataclass(frozen=True)
class DiscoveredItem:
    """One candidate document found during discover()."""
    source_uri: str
    identity: str
    hints: dict = field(default_factory=dict)


@dataclass(frozen=True)
class RawArtifact:
    """Bytes fetched from the source, before any processing."""
    item: DiscoveredItem
    content: bytes
    content_type: str
    fetched_at: datetime
    original_filename: str
    http_meta: dict = field(default_factory=dict)

    @property
    def content_hash(self) -> str:
        return "sha256:" + hashlib.sha256(self.content).hexdigest()


# ---- The connector contract -----------------------------------------------

class Connector(ABC):
    """Thin archival gateway. Discovers, fetches, and archives raw documents.

    Does NOT parse, validate, or produce structured records. That's the
    extractor's job (Layer 3).
    """

    name: str
    connector_version: str
    content_type: str         # "text/markdown", "application/pdf", etc.

    @abstractmethod
    def discover(self, ctx: "RunContext") -> Iterator[DiscoveredItem]:
        """Enumerate candidate documents. Compute stable identity keys."""

    @abstractmethod
    def derive_identity(self, source_uri: str, ctx: "RunContext") -> str:
        """Compute the stable natural key from a source URI.
        Examples: "idsr:2025:W01", "pitb_dss:2016:W11", "ajk_idsrs:2026:W18"
        This is separated from discover() so replay/re-derivation can call it
        independently.
        """

    @abstractmethod
    def fetch(self, item: DiscoveredItem, ctx: "RunContext") -> RawArtifact:
        """Retrieve the raw bytes for one item."""


# ---- Postgres handoff record (prototype) -----------------------------------

@dataclass(frozen=True)
class RawDocumentRow:
    """The row written to ingestion.raw_documents after archival.

    Does NOT carry a status field — extraction state is tracked in
    the ingestion.extractor_status table (Layer 3).
    """
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


@dataclass
class RunSummary:
    source: str
    discovered: int = 0
    fetched: int = 0
    archived: int = 0
    skipped_identity: int = 0
    skipped_content: int = 0
    errors: int = 0
    duration_ms: int = 0
```

### 1.3 The SDK runner — the template algorithm

The SDK owns the algorithm. The connector fills in the blanks. This is the Template Method pattern.

```python
# libs/chip_connectors/runner.py
from libs.chip_connectors.base import Connector, RunSummary
from libs.chip_connectors.bronze import BronzeClient
from libs.chip_connectors.dedup import DedupStore


def run_connector(conn: Connector, ctx: "RunContext") -> RunSummary:
    """
    The algorithm every connector follows. Connector authors never
    write this — they write the 3 methods it calls.
    """
    summary = RunSummary(source=conn.name)
    ctx.log.info("connector.run.started", source=conn.name)

    for item in conn.discover(ctx):
        summary.discovered += 1

        # ── Layer 1: Identity dedup ──────────────────────────────────
        if ctx.dedup.identity_seen(conn.name, item.identity):
            ctx.log.debug("connector.dedup.identity_skip",
                          identity=item.identity)
            summary.skipped_identity += 1
            continue

        # ── Layer 2: Fetch ───────────────────────────────────────────
        try:
            raw = conn.fetch(item, ctx)
            summary.fetched += 1
        except Exception as e:
            ctx.log.error("connector.fetch.failed",
                          identity=item.identity, error=str(e))
            summary.errors += 1
            continue

        # ── Layer 3: Content dedup (before archiving — avoid
        #    writing the same bytes twice) ────────────────────────────
        if ctx.dedup.content_seen(conn.name, raw.content_hash):
            ctx.log.debug("connector.dedup.content_skip",
                          identity=item.identity, hash=raw.content_hash)
            summary.skipped_content += 1
            continue

        # ── Layer 4: Archive to bronze (SDK-owned) ───────────────────
        bronze_uri = ctx.bronze.archive(
            source=conn.name,
            identity=item.identity,
            content=raw.content,
            content_type=raw.content_type,
            content_hash=raw.content_hash,
            original_filename=raw.original_filename,
            metadata={
                "source_uri": item.source_uri,
                "retrieved_at": raw.fetched_at.isoformat(),
                "connector_version": conn.connector_version,
            }
        )
        summary.archived += 1

        # ── Layer 5: Content dedup record (post-archive) ─────────────
        ctx.dedup.record_content(conn.name, raw.content_hash)

        # ── Layer 6: Signal downstream (prototype: Postgres) ─────────
        ctx.handoff.signal(RawDocumentRow(
            source=conn.name,
            identity=item.identity,
            bronze_uri=bronze_uri,
            content_hash=raw.content_hash,
            content_type=raw.content_type,
            original_filename=raw.original_filename,
            source_uri=item.source_uri,
            connector_version=conn.connector_version,
            retrieved_at=raw.fetched_at,
            file_size_bytes=len(raw.content),
        ))
        ctx.dedup.record_identity(conn.name, item.identity)

        # ── Layer 7: Seed extractor_status rows via registry ─────────
        # For each extractor registered for this source, create a
        # pending row in extractor_status so the extractor knows this
        # document exists and is ready for processing.
        registered_extractors = ctx.handoff.get_registered_extractors(conn.name)
        for extractor_name in registered_extractors:
            ctx.handoff.seed_extractor_status(
                raw_document_id=doc_id,      # returned by handoff.signal()
                extractor_name=extractor_name,
            )

    ctx.metrics.emit(summary)
    ctx.log.info("connector.run.completed", source=conn.name,
                 **summary.__dict__)
    return summary
```

### 1.4 Why no `parse` or `validate` in the connector contract

The original 02 spec has 4 abstract methods: `discover`, `fetch`, `parse`, `validate`. This design has 3: `discover`, `fetch`, `derive_identity`. The removal is deliberate:

**`parse` moves to the extractor (Layer 3).** MD table extraction belongs to a layer that understands document structure. The connector deals in bytes, not tables. This keeps the connector trivially simple (3 methods, ~30 lines each) and preserves the full document for all downstream consumers.

**`validate` splits across layers.** Record-level validation (disease ∈ vocab, cases >= 0, totals reconcile) lives in the extractor. Document-level validation (non-empty content, expected content_type) lives in the SDK runner — a zero-byte file or wrong content_type is caught at fetch time, not extracted to a downstream consumer.

### 1.5 Dedup model (prototype)

The production architecture defines three layers of dedup. The prototype uses two, backed by a simple Postgres table.

```sql
CREATE TABLE ingestion.dedup_state (
    source          TEXT NOT NULL,
    identity        TEXT NOT NULL,          -- "idsr:2025:W01"
    content_hash    TEXT NOT NULL,          -- "sha256:abc123..."
    bronze_uri      TEXT,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, identity)
);

CREATE INDEX idx_dedup_content ON ingestion.dedup_state (source, content_hash);
```

**Identity dedup:** `SELECT EXISTS(... WHERE source = ? AND identity = ?)` — prevents re-fetching `idsr:2025:W01` if it's already ingested.

**Content dedup:** `SELECT EXISTS(... WHERE source = ? AND content_hash = ?)` — catches the same file appearing under a different name. Important for DHIS where `Week_18.md`, `week_18.md`, and `Week_18(1).md` might be identical content.

The third layer (record-level dedup via Kafka message key) is downstream in the normalizer and unchanged.

### 1.6 MinIO bronze layout

```
s3://chip-bronze/
  <source>/<identity>/<content_hash>/
      <original_filename>              ← raw bytes, immutable
      .meta.json                       ← sidecar: source_uri, retrieved_at, connector_version
```

Concrete for the prototype sources:

```
chip-bronze/nih_idsr/idsr:2025:W01/sha256-abc.../Week-01-2025.md
chip-bronze/nih_idsr/idsr:2025:W01/sha256-abc.../.meta.json

chip-bronze/pitb_dss/pitb_dss:2015:W11/sha256-def.../DSS-Bulletin-Week-11.md
chip-bronze/pitb_dss/pitb_dss:2015:W11/sha256-def.../.meta.json

chip-bronze/ajk_idsrs/ajk_idsrs:2026:W18/sha256-ghi.../IDSR-WEEKLY-BULLETIN-AJK_EPI-Week-18_2026.md
chip-bronze/ajk_idsrs/ajk_idsrs:2026:W18/sha256-ghi.../.meta.json

chip-bronze/dhis_punjab/dhis_punjab:2022:W18/sha256-jkl.../week_18.md
chip-bronze/dhis_punjab/dhis_punjab:2022:W18/sha256-jkl.../.meta.json
```

**Key design properties:**
- Content-addressed (hash in path) → idempotent writes; same bytes never produce two objects
- Identity in path → human-browsable in the MinIO console
- Original filename preserved → provenance trail
- `.meta.json` sidecar captures retrieval metadata → replay doesn't need the source
- Bucket versioning ON; object-lock (WORM) ON; lifecycle: never auto-delete

### 1.7 Connector-to-extractor handoff (prototype)

```sql
CREATE TABLE ingestion.raw_documents (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source          TEXT NOT NULL,           -- "nih_idsr"
    identity        TEXT NOT NULL,           -- "idsr:2025:W01"
    bronze_uri      TEXT NOT NULL,           -- "s3://chip-bronze/nih_idsr/..."
    content_hash    TEXT NOT NULL,
    content_type    TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    source_uri      TEXT NOT NULL,
    connector_version TEXT NOT NULL,
    retrieved_at    TIMESTAMPTZ NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, identity)
);

-- Extraction state is tracked separately per (document, extractor)
-- in ingestion.extractor_status (see Layer 3: 03-document-extractors.md)
```

The extractor (Layer 3) polls for pending work:
```sql
SELECT rd.*
FROM ingestion.raw_documents rd
JOIN ingestion.extractor_status es
    ON es.raw_document_id = rd.id
WHERE es.extractor_name = 'nih_idsr_disease_tables'
  AND es.status = 'pending'
  AND rd.content_type = 'text/markdown'
ORDER BY rd.created_at;
```

### 1.8 `extractor_registry` — the handoff bridge table

The connector doesn't know which extractors exist. The `extractor_registry` table maps each source to its registered extractors. When the connector signals a new document, it queries this table and creates `extractor_status` rows for every registered extractor.

```sql
CREATE TABLE ingestion.extractor_registry (
    source          TEXT NOT NULL,
    extractor_name  TEXT NOT NULL,
    PRIMARY KEY (source, extractor_name)
);

-- Prototype seed data:
INSERT INTO ingestion.extractor_registry VALUES
    ('nih_idsr',            'nih_idsr_disease_tables'),
    ('pitb_dss',            'pitb_dss_disease_tables'),
    ('ajk_idsrs',           'ajk_idsrs_disease_tables'),
    ('dhis_punjab_weekly',  'dhis_punjab_disease_tables');
```

Adding a new extractor later:
```sql
INSERT INTO ingestion.extractor_registry VALUES ('nih_idsr', 'nih_idsr_prose');
-- Then backfill extractor_status for existing documents:
INSERT INTO ingestion.extractor_status (raw_document_id, extractor_name)
SELECT rd.id, 'nih_idsr_prose'
FROM ingestion.raw_documents rd
WHERE rd.source = 'nih_idsr';
```

The `HandoffStore` SDK module provides:
- `signal(row: RawDocumentRow) → doc_id` — INSERT into `raw_documents`
- `get_registered_extractors(source: str) → list[str]` — query `extractor_registry`
- `seed_extractor_status(raw_document_id, extractor_name)` — INSERT into `extractor_status`

### 1.9 Logging & metrics

Every connector run emits structured JSON logs via `structlog`:
```json
{"event": "connector.run.started", "source": "nih_idsr", "run_id": "a1b2c3"}
{"event": "connector.item.discovered", "identity": "idsr:2025:W01"}
{"event": "connector.item.fetched", "identity": "idsr:2025:W01", "bytes": 124800}
{"event": "connector.item.archived", "identity": "idsr:2025:W01", "bronze_uri": "s3://..."}
{"event": "connector.item.skipped.identity", "identity": "idsr:2025:W01", "reason": "already_seen"}
{"event": "connector.item.skipped.content", "identity": "idsr:2025:W01", "reason": "duplicate_content"}
{"event": "connector.item.error", "identity": "idsr:2025:W01", "error": "..."}
{"event": "connector.run.completed", "source": "nih_idsr", "discovered": 174, "archived": 170, ...}
```

Each run emits a `RunSummary` written to `ingestion.connector_runs` for operational visibility:
```sql
CREATE TABLE ingestion.connector_runs (
    run_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source          TEXT NOT NULL,
    connector_version TEXT NOT NULL,
    discovered      INT NOT NULL DEFAULT 0,
    fetched         INT NOT NULL DEFAULT 0,
    archived        INT NOT NULL DEFAULT 0,
    skipped_identity INT NOT NULL DEFAULT 0,
    skipped_content INT NOT NULL DEFAULT 0,
    errors          INT NOT NULL DEFAULT 0,
    duration_ms     INT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## 2. Per-Source Connector Specifications

### 2.1 NIH IDSR — National Weekly Epidemiological Bulletins

#### 2.1.1 Source identity

| Attribute | Value |
|---|---|
| Connector slug | `nih_idsr` |
| Source directory | `Data_sources_1/NIH/MD/` |
| Content type | `text/markdown` |
| File naming conventions | `Week-{NN}-{YYYY}.md`, `Weekly Report-{NN}-{YYYY}.md`, `IDSR-Weekly-Report-{NN}-{YYYY}.md`, `IDSRS Weekly Report-{NN}-{YYYY}.md`, `Weekly_Report_{NN}_{YYYY}.md`, `IDSR Week {NN} Bulletin ({YYYY}).md` |
| Identity format | `idsr:{year}:W{week:02d}` |
| Expected documents | ~174 unique bulletins (2021–2026) |
| Connector version | `1.0.0` |

#### 2.1.2 `discover()`

Scan `Data_sources_1/NIH/MD/` for `.md` files. Skip `.txt` files (MD is authoritative per Layer 1 decision). For each file, extract `(year, epi_week)` from the filename using a set of tolerant regexes. Skip files where year/week cannot be determined (quarantine at the connector level for manual review).

```
Identity derivation strategy:
  1. Try regex: Week-(\d{2})-(\d{4})          → group2=year, group1=week
  2. Try regex: Weekly[_ ]Report-(\d+)-(\d{4}) → group2=year, group1=week
  3. Try regex: IDSR-Weekly-Report-(\d+)-(\d{4})
  4. Try regex: IDSRS Weekly Report-(\d+)-(\d{4})
  5. Try regex: Weekly_Report_(\d+)_(\d{4})
  6. Try regex: IDSR Week (\d+) Bulletin \((\d{4})\)
  7. Fallback: open file, scan first 50 lines for "Week \d{2}, \d{4}" or "Epi[demiological ]?[Ww]eek \d{1,2}"
  8. If all fail: emit DiscoveredItem with identity="idsr:UNKNOWN", log ERROR, skip
```

**Design note:** Filename parsing is the primary strategy for NIH because 6+ patterns have been documented. However, the content-based fallback catches any new naming convention added after this document was written.

#### 2.1.3 `derive_identity()`

Must be pure — same input always yields same output. Used by the dedup store and by replay.

```python
def derive_identity(self, source_uri: str, ctx: RunContext) -> str:
    filename = os.path.basename(source_uri)
    # ... regex cascade from discover() ...
    return f"idsr:{year}:W{week:02d}"
```

#### 2.1.4 `fetch()`

Read the `.md` file from the local filesystem. For the prototype, this is `open(source_uri, 'rb').read()`. No HTTP, no retry, no rate limiting.

```python
def fetch(self, item: DiscoveredItem, ctx: RunContext) -> RawArtifact:
    path = Path(item.source_uri)
    content = path.read_bytes()
    return RawArtifact(
        item=item,
        content=content,
        content_type="text/markdown",
        fetched_at=datetime.now(timezone.utc),
        original_filename=path.name,
    )
```

#### 2.1.5 Edge cases & error handling

| Case | Behavior |
|---|---|
| `.txt` file found (not `.md`) | Skip in discover. MD is authoritative. |
| Filename year/week unparseable | Log WARN, skip, count as `discovered` but not `fetched` |
| File is zero bytes | `fetch()` returns zero-length content. Content dedup catches it if a previous zero-byte file exists. |
| Two files with same (year, week) identity | Second file hits identity dedup → skipped. The first one wins. This is correct behavior — if NIH publishes a replacement bulletin, we archive both to bronze (different content_hash) but only the first goes to `raw_documents`. A human can decide to replay the second. |
| Duplicate `.md` and `.txt` with same content | Content dedup catches this (`.txt` has no HTML tables but `content_hash` differs). Only the `.md` is ingested. |
| Files from future years (2026) | Ingested normally. Identity is `idsr:2026:W01`. The extractor/normalizer handles date validation downstream. |

#### 2.1.6 Config

```yaml
# connectors/nih_idsr/config.yaml
source: nih_idsr
connector_version: "1.0.0"
content_type: text/markdown
discovery:
  source_directory: Data_sources_1/NIH/MD
  file_extension: .md
  identity_patterns:
    - regex: "Week-(\\d{2})-(\\d{4})"
      year_group: 2
      week_group: 1
    - regex: "Weekly[_ ]Report-(\\d+)-(\\d{4})"
      year_group: 2
      week_group: 1
    - regex: "IDSR-Weekly-Report-(\\d+)-(\\d{4})"
      year_group: 2
      week_group: 1
    - regex: "IDSRS Weekly Report-(\\d+)-(\\d{4})"
      year_group: 2
      week_group: 1
    - regex: "Weekly_Report_(\\d+)_(\\d{4})"
      year_group: 2
      week_group: 1
    - regex: "IDSR Week (\\d+) Bulletin \\((\\d{4})\\)"
      year_group: 2
      week_group: 1
  content_fallback: true
  content_fallback_scan_lines: 50
  content_fallback_search_terms:
    - "Week \\d{2}, \\d{4}"
    - "Epi[demiological ]?[Ww]eek \\d{1,2}"
schedule:
  cron: "0 6 * * 2"           # Tuesdays 06:00 PKT (for future live mode)
  timezone: Asia/Karachi
freshness_max_lag_hours: 192   # weekly + publish delay slack
```

---

### 2.2 PITB-DSS — Punjab Disease Surveillance System / HRS

#### 2.2.1 Source identity

| Attribute | Value |
|---|---|
| Connector slug | `pitb_dss` |
| Source directory | `Data_sources_1/PITB-DSS/{year}/MD/` |
| Content type | `text/markdown` |
| File naming conventions | `DSS-Bulletin-Week-{N}.md` (2015), `DSS Bulletin Week {N}-{YYYY}.md` (2016), `HRS Bulletin Week {N},{YYYY}.md` (2017–2018) |
| Identity format | `pitb_dss:{year}:W{week:02d}` |
| Expected documents | ~169 unique bulletins (2015–2018) |
| Connector version | `1.0.0` |

#### 2.2.2 `discover()`

Scan all `Data_sources_1/PITB-DSS/20{YY}/MD/` subdirectories recursively. Each year is a separate subdirectory; the year is encoded in the path AND the filename.

```
Identity derivation strategy:
  1. Try regex: DSS-Bulletin-Week-(\d+)       → year from parent directory, week=group1
  2. Try regex: DSS Bulletin Week (\d+)-(\d{4}) → year=group2, week=group1
  3. Try regex: HRS Bulletin Week (\d+),(\d{4}) → year=group2, week=group1
  4. Fallback: use parent directory year hint if filename is parsable some other way
```

**Design note:** PITB-DSS is the cleanest source for identity derivation because the year is encoded in both the directory structure and the filename. The naming conventions are consistent within each year.

#### 2.2.3 File duplication handling

PITB-DSS has no known duplicate files (unlike DHIS). Each week appears once per year with exactly one file. The identity dedup at the connector level is sufficient.

#### 2.2.4 Config

```yaml
# connectors/pitb_dss/config.yaml
source: pitb_dss
connector_version: "1.0.0"
content_type: text/markdown
discovery:
  source_directory: Data_sources_1/PITB-DSS
  file_extension: .md
  recursive: true
  identity_patterns:
    - regex: "DSS-Bulletin-Week-(\\d+)"
      year_from_path: true
      week_group: 1
    - regex: "DSS Bulletin Week (\\d+)-(\\d{4})"
      year_group: 2
      week_group: 1
    - regex: "HRS Bulletin Week (\\d+),(\\d{4})"
      year_group: 2
      week_group: 1
  incomplete_years:
    2015: "Weeks 3-52 (Weeks 1-2 missing)"
    2018: "Weeks 1-26 (partial year)"
schedule:
  cron: "0 6 * * 3"           # Wednesdays (legacy source, no new data expected)
  timezone: Asia/Karachi
freshness_max_lag_hours: 8760  # legacy — no freshness SLO, archival only
```

---

### 2.3 AJK IDSRS — AJ&K Provincial Weekly Bulletins

#### 2.3.1 Source identity

| Attribute | Value |
|---|---|
| Connector slug | `ajk_idsrs` |
| Source directory | `Data_sources_1/AJK/out/MD/` |
| Content type | `text/markdown` |
| File naming convention | `IDSR-WEEKLY-BULLETIN-AJK_EPI-Week-{NN}_{YYYY}.md` |
| Identity format | `ajk_idsrs:{year}:W{week:02d}` |
| Expected documents | 3 bulletins (2026, Weeks 18–20) |
| Connector version | `1.0.0` |

#### 2.3.2 Identity derivation

```python
# Single, clean pattern — the easiest source
pattern = r"IDSR-WEEKLY-BULLETIN-AJK_EPI-Week-(\d+)_(\d{4})"
identity = f"ajk_idsrs:{year}:W{week:02d}"
```

#### 2.3.3 Config

```yaml
# connectors/ajk_idsrs/config.yaml
source: ajk_idsrs
connector_version: "1.0.0"
content_type: text/markdown
discovery:
  source_directory: Data_sources_1/AJK/out/MD
  file_extension: .md
  identity_patterns:
    - regex: "IDSR-WEEKLY-BULLETIN-AJK_EPI-Week-(\\d+)_(\\d{4})"
      year_group: 2
      week_group: 1
schedule:
  cron: "0 6 * * 2"
  timezone: Asia/Karachi
freshness_max_lag_hours: 192
```

---

### 2.4 DHIS Punjab — Weekly Feedback Reports (DHIS-II era, 2022)

#### 2.4.1 Source identity

| Attribute | Value |
|---|---|
| Connector slug | `dhis_punjab_weekly` |
| Source directory | `Data_sources_1/DHIS/MD/` |
| Content type | `text/markdown` |
| File naming convention | `Week_{NN}.md`, `week_{NN}.md`, `Week_{NN}({N}).md`, `week_{NN}({N}).md` |
| Identity format | `dhis_punjab_weekly:{year}:W{week:02d}` |
| Expected documents | ~52 unique weeklies from 2022 (DHIS-II era only) |
| Connector version | `1.0.0` |

#### 2.4.2 The year disambiguation problem

DHIS filenames do NOT encode the year. `Week_18.md` could be 2022, 2024, or 2025. Per Layer 1 decision L1-04, the connector opens each file and extracts the year from content during `discover()`.

```
Discovery strategy for DHIS:
  For each .md file:
    1. Parse week number from filename (regex: [Ww]eek[_ ]?(\d+))
    2. Open file, scan first 30 lines
    3. Search for year patterns:
       - "20\d{2}" (any 4-digit year starting with 20)
       - "DHIS-II" → 2022 era
       - "DHIS2"  → 2024–2025 era
       - Date range like "May 2-8" → cross-reference with week calendar
    4. If year == 2022 and content contains district-level disease tables:
         → DHIS-II era → ingest
       Elif year ∈ {2024, 2025}:
         → DHIS2 era → SKIP (province aggregates only, deferred per L1-07)
       Else:
         → identity = "dhis_punjab_weekly:UNKNOWN:W{NN}", log WARN
```

**Design note:** The connector opens files during discover. This is an intentional deviation from the "discover is cheap" ideal because DHIS naming makes it unavoidable. ~111 files at ~23 KB each = ~2.5 MB of reading — negligible. The extracted year is cached in memory for the run and passed via `DiscoveredItem.hints`.

#### 2.4.3 Config

```yaml
# connectors/dhis_punjab_weekly/config.yaml
source: dhis_punjab_weekly
connector_version: "1.0.0"
content_type: text/markdown
discovery:
  source_directory: Data_sources_1/DHIS/MD
  file_extension: .md
  identity_patterns:
    - regex: "[Ww]eek[_ ]?(\\d+)"
      week_group: 1
      year_from_content: true
  content_year_extraction:
    scan_lines: 30
    year_regex: "(20\\d{2})"
    era_hints:
      - pattern: "DHIS-II"
        year: 2022
      - pattern: "DHIS2|DHIS-2"
        year_hint: "skip"        # DHIS2 era → deferred, not ingested yet
  fallback_behavior: skip        # If year can't be determined, skip (don't guess)
schedule:
  cron: "0 6 * * 3"
  timezone: Asia/Karachi
freshness_max_lag_hours: 8760    # legacy source, archival only
```

---

## 3. Format-Change Resilience

### 3.1 The layer separation is the primary defence

The thin connector model inherently protects against format changes. The connector archives bytes — it doesn't care if the document structure changes. A format change that breaks the extractor (Layer 3) doesn't affect the connector at all. The raw document is already safe in bronze.

```
Before format change:                      After format change:
═══════════════════════                    ═══════════════════════
Connector: archives Week-01-2025.md        Connector: archives Week-01-2026.md
           ✅ success                                ✅ success (same code, same logic)

Extractor: parses table layout A           Extractor: encounters table layout B
           ✅ success                                ❌ quarantine (unrecognized layout)
                                                     
                                           Fix: update extractor layout adapter
                                           Replay: re-process from bronze
                                           ✅ success (no re-fetch needed)
```

### 3.2 What can change without breaking anything

| Change | Connector impact | Extractor impact |
|---|---|---|
| New file naming convention | Add regex to identity_patterns in config. Redeploy connector. | None. |
| Document layout changes | None. Connector doesn't parse. | Extractor encounters new layout → quarantine. Fix extractor, replay from bronze. |
| New diseases added to tables | None. | Extractor encounters new disease label → quarantine if not in vocab, pass if validation accepts. |
| New provinces/districts in tables | None. | Extractor encounters new district name → quarantine if not in gazetteer. |
| Filename encoding change | Could break identity derivation. Config change. | None. |
| Source URL change (future live mode) | Update config: `discovery.listing_url`. Redeploy. | None. |

### 3.3 Identity stability — the contract with downstream

The identity key (`idsr:2025:W01`) is the stable reference that links the connector, the `raw_documents` table, the bronze artifact, and every extractor and normalizer that processes this document. It MUST be:

1. **Derivable from the source URI alone** — no database lookup required to compute it
2. **Stable across connector redeployments** — same URI always yields the same identity
3. **Opaque to downstream** — the extractor treats it as an opaque string key
4. **Content-independent** — derived from metadata (filename, URL, path), not from document contents

This guarantees that the extractor can always find the bronze artifact for a given identity without the connector being available.

---

## 4. Monorepo Layout

```
libs/
  chip_connectors/                    # SDK (shared, written once)
    __init__.py
    base.py                           # Connector ABC, DiscoveredItem, RawArtifact,
                                      #   RawDocumentRow, RunSummary
    runner.py                         # run_connector() — the algorithm
    bronze.py                         # BronzeClient.archive() — MinIO put + .meta.json sidecar
    dedup.py                          # DedupStore — identity + content dedup (Postgres-backed)
    handoff.py                        # HandoffStore.signal() — INSERT INTO raw_documents
    logging.py                        # structlog configuration + context binding
    metrics.py                        # RunSummary → connector_runs table

connectors/                           # One package per source (thin)
  nih_idsr/
    __init__.py
    connector.py                      # NihIdsrConnector(Connector)
    config.yaml
  pitb_dss/
    __init__.py
    connector.py                      # PitbDssConnector(Connector)
    config.yaml
  ajk_idsrs/
    __init__.py
    connector.py                      # AjkIdsrsConnector(Connector)
    config.yaml
  dhis_punjab_weekly/
    __init__.py
    connector.py                      # DhisPunjabWeeklyConnector(Connector)
    config.yaml
  README.md                           # "How to add a connector" guide

pipelines/
  ingestion/
    assets.py                         # Dagster SDAs: raw_nih_idsr, raw_pitb_dss, etc.
    resources.py                      # Dagster resource → RunContext (MinIO, Postgres, etc.)
    partitions.py                     # WeeklyPartitionsDefinition per source
    schedules.py                      # Cron schedules per source
    checks.py                         # Asset checks: freshness, zero-discovery guard

infra/
  docker/
    Dockerfile.connector              # Python 3.12 + MinIO client + structlog + dagster
  compose/
    docker-compose.ingestion.yaml     # MinIO, Postgres (dedup + raw_documents)
```

**"Add a connector" contract** (the README):
1. Copy an existing `connectors/<source>/` folder
2. Implement `discover()`, `fetch()`, `derive_identity()`
3. Write `config.yaml`
4. Register a Dagster asset in `pipelines/ingestion/assets.py`
5. Run the connector; verify rows appear in `ingestion.raw_documents`

---

## 5. Dagster Integration

### 5.1 Assets per source

```python
# pipelines/ingestion/assets.py
from dagster import asset, AssetExecutionContext

@asset(
    group_name="ingestion",
    kinds={"minio", "postgres"},
)
def raw_nih_idsr(context: AssetExecutionContext, connector_ctx: RunContext):
    from connectors.nih_idsr.connector import NihIdsrConnector
    summary = run_connector(NihIdsrConnector(), connector_ctx)
    context.add_output_metadata({
        "discovered": summary.discovered,
        "archived": summary.archived,
        "skipped_identity": summary.skipped_identity,
        "skipped_content": summary.skipped_content,
        "errors": summary.errors,
    })

@asset(group_name="ingestion", kinds={"minio", "postgres"})
def raw_pitb_dss(context, connector_ctx):
    from connectors.pitb_dss.connector import PitbDssConnector
    summary = run_connector(PitbDssConnector(), connector_ctx)
    context.add_output_metadata({...})

# ... ajk_idsrs, dhis_punjab_weekly ...
```

### 5.2 Resources

```python
# pipelines/ingestion/resources.py
from dagster import resource, InitResourceContext
from libs.chip_connectors.runner import RunContext
from libs.chip_connectors.bronze import BronzeClient
from libs.chip_connectors.dedup import DedupStore
from libs.chip_connectors.handoff import HandoffStore

@resource
def connector_ctx_resource(context: InitResourceContext) -> RunContext:
    return RunContext(
        bronze=BronzeClient(
            endpoint=os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            bucket="chip-bronze",
        ),
        dedup=DedupStore(
            db_url=os.environ["DATABASE_URL"],
        ),
        handoff=HandoffStore(
            db_url=os.environ["DATABASE_URL"],
        ),
        log=structlog.get_logger(),
    )
```

### 5.3 Backfill

The prototype is a one-time historical backfill. Each source is a Dagster partitioned asset:

```python
# pipelines/ingestion/partitions.py
from dagster import WeeklyPartitionsDefinition

nih_idsr_partitions = WeeklyPartitionsDefinition(
    start_date="2021-01-04",   # First epi-week of 2021
    end_date="2026-06-30",
)

pitb_dss_partitions = WeeklyPartitionsDefinition(
    start_date="2015-01-19",   # Week 3, 2015
    end_date="2018-06-30",
)
```

Running the backfill:
```bash
dagster asset backfill --asset raw_nih_idsr --partitions "*"
```

The connector's dedup makes re-running any partition a no-op — identity-seen skips already-archived items.

---

## 6. Open Questions & Design Decisions

### 6.1 Resolved

| ID | Decision | Rationale |
|---|---|---|
| **L2-01** | Connector is thin: `discover → fetch → archive → signal`. No parse, no validate. | Parse moves to extractor (Layer 3) so the full document is preserved for multiple downstream consumers. |
| **L2-02** | Connector→extractor handoff uses Postgres `ingestion.raw_documents` table (prototype). | Simpler than Kafka for the prototype. Graduates to Kafka `document_ingested` topic for live mode. |
| **L2-03** | SDK uses Template Method pattern — `run_connector()` owns the algorithm, connector fills in the blanks. | Adding a source = 3 methods + config. Framework code never changes. |
| **L2-04** | MD is authoritative over TXT. `.txt` files are skipped in `discover()`. | MD preserves HTML table structure; TXT loses it. Decision from Layer 1. |
| **L2-05** | Identity is derived from filename/URI, not content. Internal to the connector but opaque to downstream. | Content-independent identity keeps the extractor from needing to re-derive what the connector already computed. |
| **L2-06** | DHIS year disambiguation happens during `discover()` by opening files and scanning content. | DHIS filenames don't encode the year. ~111 files at ~23 KB = ~2.5 MB of reading — negligible. |
| **L2-07** | DHIS2 era (2024–2025) files are skipped during discover. | DHIS2 has only province-level aggregates — deferred per Layer 1 decision L1-07. |
| **L2-11** | DHIS year ambiguity: first year found in first 30 lines wins. | Good enough for prototype. Can refine later if multi-year references become a problem. |
| **L2-12** | `derive_identity()` stays as a method on the Connector ABC, not a standalone function. | Keeps the identity logic self-contained within the connector class alongside config. Testability isn't a meaningful difference for a pure function at prototype scale. |
| **L2-13** | Duplicate identities: first one wins, second is dedup-skipped. No special handling. | Bulletins are not expected to have updates. If a bulletin is republished with corrections, it will have a new identity (different week number or explicit revision marker). |

### 6.2 Deferred

| ID | Question | Why deferred |
|---|---|---|
| **L2-08** | When `fetch()` becomes HTTP, how does the SDK rate-limiter work for local file reads? | For local files, rate limiting is disabled. The SDK's `http.py` module is a stub in the prototype. |
| **L2-09** | Should the connector archive `.txt` files as well for completeness? | MD is authoritative per Layer 1. TXT files could be archived as a separate pass if needed for validation. Deferred. |
| **L2-10** | How should the connector handle files that fail identity derivation (unknown naming convention)? | Currently: log WARN, skip. Alternatively: archive with identity `UNKNOWN`, let human triage. |

### 6.3 Open

*None remaining at Layer 2.*

---

## Appendix A: Connector Output Schema (`ingestion.raw_documents`)

| Column | Type | Description | Example |
|---|---|---|---|
| `id` | `BIGINT` | Surrogate key | `1` |
| `source` | `TEXT` | Connector slug | `"nih_idsr"` |
| `identity` | `TEXT` | Stable natural key | `"idsr:2025:W01"` |
| `bronze_uri` | `TEXT` | MinIO object path | `"s3://chip-bronze/nih_idsr/idsr:2025:W01/sha256-abc.../Week-01-2025.md"` |
| `content_hash` | `TEXT` | SHA-256 of raw bytes | `"sha256:9f3a86..."` |
| `content_type` | `TEXT` | MIME type | `"text/markdown"` |
| `original_filename` | `TEXT` | Filename as found on disk | `"Week-01-2025.md"` |
| `source_uri` | `TEXT` | Full path to original file | `"Data_sources_1/NIH/MD/Week-01-2025.md"` |
| `connector_version` | `TEXT` | Semver of connector code | `"1.0.0"` |
| `retrieved_at` | `TIMESTAMPTZ` | When the file was read | `"2026-07-15T10:30:00Z"` |
| `file_size_bytes` | `BIGINT` | Size of raw content | `124800` |
| `created_at` | `TIMESTAMPTZ` | When row was inserted | `"2026-07-15T10:30:30Z"` |

---

## Appendix B: Document History

| Date | Author | Change |
|---|---|---|
| 2026-07-15 | Architecture team | Initial Layer 2 zoomed-in design — thin connector model for prototype |
| 2026-07-15 | Architecture team | Resolved L2-11, L2-12, L2-13. All open questions closed. |
| 2026-07-15 | Architecture team | Reconciled with Layer 3: removed `status` column from `raw_documents`, added `extractor_registry`, connector now seeds `extractor_status` rows. |
