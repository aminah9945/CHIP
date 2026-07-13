# 02 — Data Ingestion & Connector Framework

**Subsystem:** Ingestion & Connectors
**Project:** CHIP (Climate–Health Intelligence Platform), PCN Research Group, NUCES/FAST Islamabad
**Status:** Design (Phase 1 — public/historical data only; institutional MOUs pending)
**Author:** Ingestion architecture working group
**Last updated:** 2026-07-13
**Governing ADRs:** ADR-001 (event-driven topology), ADR-002 (Kafka everywhere / Spark only for news NLP / **low-frequency consumers are Dagster batch drains**), ADR-003 (Postgres+PostGIS+Timescale+pgvector / MinIO bronze / Neo4j), ADR-004 (self-hosted Docker Compose), ADR-005 (canonical grain = district × epi-week, provenance on every record), **ADR-012 (agentic parse, cached to bronze)**, **ADR-013 (news via NAaaS API)**, **ADR-015 (boundary versioning + revision detection)**.

> This document designs *within* the locked ADRs. It does not relitigate topology, stack, storage engines, or infra. Where a source pushes against an ADR (e.g. PMD has no open API), the resolution is a connector-level workaround, not an architecture change.

> ### ⚠️ Revision note (2026-07-13) — read before implementing
>
> Two premises in the original draft turned out to be **false**, and the sections that rested on them are superseded:
>
> | Section | Original premise | Reality | Superseded by |
> |---|---|---|---|
> | **§2.1, §2.3** (PDF parsing) | Institutional PDFs have a digital text layer; `pdfplumber` → `camelot` is the primary extraction path, LLM is a fallback | **They require an agentic parse.** The deterministic tiers do not work on these documents. | **ADR-012** — agentic parse is primary; every result is **cached immutably to bronze**; deterministic extractors are demoted to cross-validators |
> | **§2.4** (news) | CHIP operates six per-outlet RSS/HTML scrapers (Dawn, Tribune, express.pk, Jang, BBC Urdu, Geo) | **News comes from the lab's own NAaaS platform** via a keyword + date-range query API | **ADR-013** — one `naaas` connector; six scrapers retired; the relevance gate comes for free |
>
> §1 (the SDK), §3 (Dagster), §4 (replay), §5 (quarantine), §6 (layout/config) remain valid and are **more** important than before — an agentic parser fails by *hallucinating plausible numbers*, not by returning nothing, so **validation and total-reconciliation are now the primary defence, not a safety net.**

---

## 0. Scope and mental model

The ingestion subsystem is the set of **coarse-grained connector deployables** that sit at the left edge of ADR-001's pipeline:

```
   ┌─────────────┐   raw bytes    ┌────────┐   parsed+validated JSON   ┌─────────┐
   │ connectors/ │ ─────────────▶ │ MinIO  │                          │  Kafka  │
   │  (this doc) │                │ bronze │◀── archive-first ────────│ topics  │
   └─────────────┘                └────────┘                          └─────────┘
        │  discover → fetch → archive-raw → parse → validate → produce        │
        │  orchestrated by Dagster (assets/jobs), NOT Kafka Connect           ▼
        └──────────────────────────────────────────────▶  normalizers → enrichers → serving app
```

**One firm rule (ADR-003 + ADR-005 realised at ingestion):** *archive raw bytes to MinIO bronze **before** parsing.* The connector never throws away what it fetched. Parsing/validation happen against the archived object, so any parser improvement can be replayed from bronze without re-hitting the source (see §4). Every message that reaches Kafka carries a provenance envelope (§1.6).

**Design priorities (team reality):** correctness > reproducibility > maintainability > throughput. Volume is low (low-thousands of news articles/day; weekly/daily bulletins). We optimise for a rotating cohort of mostly-Python MS/PhD students being able to add a new source in a day by copying an existing connector.

---

## 1. Connector SDK (`libs/chip_connectors`)

### 1.1 Why custom Python and not Kafka Connect

This is a deliberate deviation from the "obvious" streaming default, justified for *this* team and *these* sources:

| Factor | Kafka Connect | Custom Python SDK (chosen) |
|---|---|---|
| Source shape | Assumes JDBC/CDC/S3/HTTP source connectors with steady record streams | Our sources are **PDFs behind WordPress pages, ad-hoc HTML, RSS, paid met data, one-off HDX CSVs** — no off-the-shelf connector fits |
| Transform expressiveness | SMTs are deliberately weak; PDF table extraction / Urdu normalisation / LLM-assisted parsing is impossible in a SMT | Full Python: `pdfplumber`, `camelot`, `trafilatura`, `pandas`, an LLM client |
| Team skills | Requires Connect/JVM/converter/SMT operational knowledge — rare in a rotating student team | Everyone already writes Python |
| Cadence | Built for continuous streams; awkward for "run once a week, one PDF" | Dagster schedules a plain Python function (ADR-002 says weekly/daily sources are plain Python consumers/producers orchestrated by Dagster) |
| Provenance/replay | Would need custom SMTs + external store to satisfy ADR-005 provenance and MinIO archival | Archival + provenance are first-class in the SDK |
| Ops surface | Extra JVM service, connector plugins, distributed workers to run and debug | One dependency (`kafka-python`/`confluent-kafka`), no extra cluster component |

Kafka *itself* stays the backbone for all sources per ADR-002 — we just produce to it with a thin, well-tested Python client instead of Connect. **Rule of thumb we adopt:** if a future source ever *is* a clean relational DB or S3 bucket that Connect supports natively, we may run a single Connect worker for it; until then, the SDK is the standard.

### 1.2 Lifecycle contract

Every connector implements the same six-stage lifecycle. The SDK provides the scaffolding (archival, hashing, retry, DLQ, metrics, logging); the connector author fills in only the source-specific stages (`discover`, `fetch`, `parse`, `validate`).

```
discover ─▶ fetch ─▶ archive_raw ─▶ parse ─▶ validate ─▶ produce
   │          │          │            │          │           │
   │          │          │            │          │           └─ Kafka topic (per source)
   │          │          │            │          └─ pydantic model check; failures → quarantine (§5)
   │          │          │            └─ bytes → structured records
   │          │          └─ SDK: write immutable object to MinIO bronze + return object key
   │          └─ HTTP GET with retry/backoff/rate-limit (SDK-managed session)
   └─ list candidate items (PDF links, RSS entries, API pages) + compute their identity keys
```

Stages `archive_raw` and `produce` are **SDK-owned** (authors never write MinIO or Kafka code directly). This is what keeps provenance and idempotency uniform across 20+ connectors written by different students.

### 1.3 Core interfaces (Python sketches)

```python
# libs/chip_connectors/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator, Sequence, Protocol
import hashlib

# ---- Value objects -------------------------------------------------------

@dataclass(frozen=True)
class DiscoveredItem:
    """One candidate unit of work found during discover()."""
    source_uri: str                 # canonical URL/API id to fetch
    identity: str                   # stable natural key, e.g. "idsr:2025:W39"
    hints: dict = field(default_factory=dict)   # e.g. {"epi_week": 39, "year": 2025}

@dataclass(frozen=True)
class RawArtifact:
    """Bytes fetched from the source, before any parsing."""
    item: DiscoveredItem
    content: bytes
    content_type: str               # "application/pdf", "text/html", "application/json"
    fetched_at: datetime
    http_meta: dict = field(default_factory=dict)   # status, etag, last-modified

    @property
    def content_hash(self) -> str:
        return "sha256:" + hashlib.sha256(self.content).hexdigest()

@dataclass(frozen=True)
class SourceRecord:
    """One normalised record ready to be enveloped and produced to Kafka.
    NOT yet canonical-grain (that is the normalizer's job) — just structured
    and self-describing at the source's own granularity."""
    payload: dict                   # parsed fields, source-native schema
    record_key: str                 # Kafka message key (dedup/ordering), e.g. district+disease+week
    occurred_at: datetime | None    # event time if known (article ts, sitrep period end)

# ---- Provenance envelope (ADR-005) --------------------------------------

@dataclass(frozen=True)
class Provenance:
    source: str                     # "nih_idsr"
    connector_version: str          # semver of the connector code
    transform_version: str          # semver of parse+validate logic (drives replay, §4)
    retrieved_at: datetime
    raw_object_key: str             # MinIO bronze key of the RawArtifact
    content_hash: str               # sha256 of raw bytes
    source_uri: str
    identity: str

# ---- The connector contract ---------------------------------------------

class Connector(ABC):
    name: str                       # "nih_idsr" -> also the Kafka topic base + bronze prefix
    connector_version: str
    transform_version: str
    kafka_topic: str                # "chip.raw.nih_idsr"

    @abstractmethod
    def discover(self, ctx: "RunContext") -> Iterator[DiscoveredItem]:
        """Enumerate candidate items. Apply watermark filtering here or let
        the SDK do it via already_seen()."""

    @abstractmethod
    def fetch(self, item: DiscoveredItem, ctx: "RunContext") -> RawArtifact:
        """Retrieve raw bytes. Use ctx.http for retry/backoff/rate-limit."""

    @abstractmethod
    def parse(self, raw: RawArtifact, ctx: "RunContext") -> Sequence[SourceRecord]:
        """Bytes -> structured records. May call ctx.pdf / ctx.llm helpers."""

    @abstractmethod
    def validate(self, rec: SourceRecord, ctx: "RunContext") -> None:
        """Raise ValidationError to route the record to quarantine (§5).
        Return None to accept."""
```

The **SDK driver** ties the stages together so authors never re-implement archival/dedup/DLQ:

```python
# libs/chip_connectors/runner.py
def run_connector(conn: Connector, ctx: RunContext) -> RunSummary:
    summary = RunSummary(source=conn.name)
    for item in conn.discover(ctx):
        if ctx.dedup.already_seen(conn.name, item.identity, ctx.watermark):
            summary.skipped += 1
            continue
        try:
            raw = _with_retry(conn.fetch, item, ctx)          # §1.4
        except FetchError as e:
            ctx.dlq.send(conn.name, item, stage="fetch", error=e)   # §1.5
            summary.dlq += 1
            continue

        # ARCHIVE-FIRST: bronze write happens before parse, always.
        object_key = ctx.bronze.put(conn.name, raw)           # §1.7
        if ctx.dedup.content_seen(conn.name, raw.content_hash):
            summary.duplicate_content += 1                    # same bytes, skip reprocess
            ctx.watermark.advance(conn.name, item)
            continue

        prov = Provenance(
            source=conn.name, connector_version=conn.connector_version,
            transform_version=conn.transform_version, retrieved_at=raw.fetched_at,
            raw_object_key=object_key, content_hash=raw.content_hash,
            source_uri=item.source_uri, identity=item.identity)

        for rec in conn.parse(raw, ctx):
            try:
                conn.validate(rec, ctx)
            except ValidationError as e:
                ctx.quarantine.send(conn.name, rec, prov, error=e)   # §5
                summary.quarantined += 1
                continue
            ctx.producer.produce(conn.kafka_topic, envelope(rec, prov))
            summary.produced += 1

        # ── CRITICAL (fixed 2026-07-13) ───────────────────────────────────────
        # confluent-kafka's produce() is ASYNCHRONOUS — it buffers locally.
        # We must not mark this content as "produced" until the broker has
        # acknowledged every message. Without this flush, a crash between the
        # last produce() and the buffer drain LOSES those messages while
        # record_content()/watermark.advance() tell the next run to skip them:
        # silent, permanent data loss with no error anywhere.
        ctx.producer.flush()                    # raises if any delivery failed
        # ──────────────────────────────────────────────────────────────────────

        ctx.dedup.record_content(conn.name, raw.content_hash)
        ctx.watermark.advance(conn.name, item)
    ctx.metrics.emit(summary)                                 # §1.8
    return summary
```

**Ordering rule (non-negotiable):** `flush() → record_content() → watermark.advance()`. Each step may only claim success once the step before it has *durably* succeeded. If `flush()` raises, the content is **not** recorded and the watermark does **not** advance, so the next run re-produces the whole item. Duplicates on Kafka are harmless — the normalizer UPSERTs idempotently on `record_key` (§1.4 layer 3) — whereas a lost message is invisible and unrecoverable. **Prefer at-least-once and lean on idempotency; never risk at-most-once.**

`RunContext` is the injected bag of SDK services (`http`, `bronze`, `producer`, `dedup`, `watermark`, `dlq`, `quarantine`, `metrics`, `log`, plus `parse_cache` (ADR-012) and optional `pdf`/`llm` helpers). Dagster builds it as a resource (§3).

### 1.4 Idempotency, dedup, watermarks, retry

**Three layers of dedup**, because our sources re-publish and re-list constantly:

1. **Identity dedup (item-level):** `already_seen(source, identity, watermark)` — the natural key (`idsr:2025:W39`, article canonical URL, `ndma:monsoon2025:sitrep-67`). Prevents re-fetching the same logical item. Backed by a small `ingestion.seen_items` table in Postgres (source, identity, first_seen, last_content_hash).
2. **Content dedup (byte-level):** `content_seen(source, sha256)` — if the exact bytes were already archived and produced, skip. Catches "same PDF re-uploaded with a new `?v=` cache-buster", identical RSS re-runs. Backed by `ingestion.seen_content`.
3. **Record dedup (downstream):** Kafka message **key** = `record_key` (e.g. `district|disease|epi_week`). Combined with Kafka log compaction on `chip.raw.*` where appropriate, and idempotent UPSERTs in the normalizer, this makes the whole pipeline **at-least-once with effective exactly-once semantics** at the canonical grain.

**Watermarks** are per-source high-water marks so `discover()` can stop early and backfills are bounded:
- Time-based sources (news RSS, sitreps): watermark = latest `occurred_at`/publish time successfully produced.
- Enumerable sources (IDSR weeks): watermark = `(year, epi_week)` of last complete bulletin.
Stored in `ingestion.watermarks(source, watermark_json, updated_at)`; advanced only after successful produce.

**Retry/backoff** (SDK, wraps `fetch`):
```python
# libs/chip_connectors/http.py
DEFAULT_RETRY = RetryPolicy(
    max_attempts=5,
    backoff=ExponentialBackoff(base=2.0, initial=1.0, max_delay=60.0, jitter=True),
    retry_on=(TimeoutError, HTTP5xx, ConnectionError, HTTP429),
    give_up_on=(HTTP404, HTTP401, HTTP403),   # -> straight to DLQ, no retry
)
```
`HTTP429` additionally honours `Retry-After`. After `max_attempts`, the item goes to the DLQ (§1.5), not silently dropped.

**Rate limiting** (SDK, per-host token bucket) — critical for polite scraping of Dawn/Tribune/NIH/NDMA:
```python
# config-driven, per source; see §6 config example
rate_limit:
  requests_per_minute: 20
  max_concurrency: 2
  min_interval_ms: 500        # hard floor between requests to same host
  respect_retry_after: true
```
The SDK maintains one limiter per registered host and shares it across connectors that hit the same host.

### 1.5 Dead-letter handling

Two distinct failure channels — **do not conflate them**:

- **DLQ (operational failure):** fetch failed after retries, MinIO write failed, source 5xx. The *item* is dead, we may retry later. Written to Kafka topic `chip.dlq.<source>` **and** row in `ingestion.dead_letter(source, identity, stage, error, http_meta, first_failed, attempts, resolved)`. Dagster surfaces DLQ depth as an alert (§3.4). Reprocessing is a Dagster job that replays unresolved DLQ rows.
- **Quarantine (data-quality failure):** fetch+archive succeeded but `parse`/`validate` rejected the record — usually **schema drift** (§5). The raw bytes are safe in bronze; the *record* goes to `chip.quarantine.<source>` + `ingestion.quarantine` table with the offending payload and validation error. A human triages, fixes the parser, bumps `transform_version`, and replays from bronze (§4).

Envelope for both includes full `Provenance` so triage never needs to guess where the bytes are.

### 1.6 Kafka message envelope (provenance, ADR-005)

> **Reconciled (ADR-007):** the *wire* envelope is **CloudEvents 1.0** (subsystem 01 §3.1) and topics are
> named `chip.<domain>.<source>.<entity>.v<major>` (e.g. `chip.health.nih_idsr.disease_case_report.v1`),
> **not** the flat `chip.raw.<source>` form shown below. The `Provenance` dataclass here is retained as the
> in-process representation and is serialized into the CloudEvents `data.provenance` block at produce time.
> The sketch below is kept for its field-level content; read it as the payload/provenance content, not the
> envelope format.

Every message on `chip.raw.*` is:
```json
{
  "schema_version": "1",
  "provenance": {
    "source": "nih_idsr",
    "connector_version": "1.4.0",
    "transform_version": "2.1.0",
    "retrieved_at": "2026-07-08T04:12:00Z",
    "raw_object_key": "nih_idsr/2025/W39/sha256-ab12.../report.pdf",
    "content_hash": "sha256:ab12...",
    "source_uri": "https://nih.org.pk/.../Weekly_Report-39-2025.pdf",
    "identity": "idsr:2025:W39"
  },
  "record_key": "punjab|lahore|dengue|2025-W39",
  "occurred_at": "2025-09-28T00:00:00Z",
  "payload": { "...source-native fields..." }
}
```
Serialized JSON in Phase 1 (human-debuggable, students can `kafka-console-consumer` it). Schema is enforced by a shared **pydantic** `Envelope` model, and a JSON Schema is registered in `libs/chip_schemas/` for cross-language validation. Migration to Avro/Protobuf + Schema Registry is a documented Phase-2 option, deferred because JSON at low thousands of msgs/day costs nothing and aids debuggability.

**Topic naming — the authoritative form is ADR-007's** `chip.<domain>.<source>.<entity>.v<major>`. The flat `chip.raw.<source>` names used elsewhere in this document are **superseded shorthand**; read them as the payload content, not the topic name.

```
chip.health.nih_idsr.disease_case_report.v1
chip.weather.openmeteo.district_daily_obs.v1
chip.hazard.ndma.disaster_event.v1
chip.media.naaas.article.v1                  ← ONE topic (ADR-013). Per-outlet topics are retired.
chip.media.enriched.media_signal.v1          ← Spark NLP output
chip.<domain>.<source>.<entity>.dlq.v1
chip.<domain>.<source>.<entity>.quarantine.v1
```

**Outlet is now a *field*, not a topic** (ADR-013): news arrives from one source (NAaaS), so the Spark NLP enricher subscribes to a single topic instead of a wildcard over six. Structured sources are consumed by **Dagster batch drains** (ADR-002 §A3) — bounded reads from last-committed-offset to high-water-mark, one per asset materialization — **not** long-running consumer processes. A daemon that idles all week to process one PDF is a liability, and it makes Dagster's asset graph a lie about what actually ran.

### 1.7 MinIO bronze layout (immutable raw + cached parses)

> **Reconciled (2026-07-13):** this layout and subsystem 01 §4.1's layout disagreed (`<hash>/<filename>` vs `<ts>__<hash>.<ext>`). **This one wins** — the trailing directory is what lets a cached parse artifact (ADR-012) sit beside the bytes it was derived from. 01 §4.1 is superseded.

```
s3://chip-bronze/
  <source>/<partition.../><content_hash>/
      <original_filename>              ← the raw bytes, immutable
      parse/<parser_id>@<version>.json ← cached parse output (ADR-012), immutable
```
Concrete:
```
chip-bronze/nih_idsr/2025/W39/sha256-ab12.../Weekly_Report-39-2025.pdf
chip-bronze/nih_idsr/2025/W39/sha256-ab12.../parse/llamaparse@1.json     ← agentic parse, cached
chip-bronze/nih_idsr/2025/W39/sha256-ab12.../parse/pdfplumber@1.json     ← cross-check
chip-bronze/ndma_sitrep/monsoon2025/sitrep-67/sha256-cd34.../report.pdf
chip-bronze/naaas/2026/07/08/sha256-ef56.../response.json                ← NAaaS API response
chip-bronze/pmd_weather/openmeteo/2026/07/08/sha256-.../lahore.json
```
- **Content-addressed** (hash in the path) → writes are idempotent and immutable; the same bytes never produce two objects.
- **Cached parses are keyed by `(content_hash, parser_id@version)` and are never overwritten** (ADR-012). This is what makes replay deterministic despite a non-deterministic parser, and what caps the parser bill at exactly one call per document, forever.
- Object metadata carries `source_uri`, `retrieved_at`, `content_type`, `connector_version`.
- Bucket versioning ON; **object-lock (WORM)** on `chip-bronze`; lifecycle policy: **never auto-delete bronze** — it is the system of record for replay (§4), and Kafka is only transport.

### 1.8 Structured logging & metrics

- **Logging:** `structlog` JSON logs, always bound with `source`, `run_id`, `identity`, `stage`, `transform_version`. One log line per stage transition; ERROR lines carry the bronze key so an engineer can pull the exact bytes.
- **Metrics:** each run emits a `RunSummary` (discovered, fetched, produced, skipped, duplicate_content, dlq, quarantined, duration). Exposed as Prometheus counters/gauges via a pushgateway (batch jobs are short-lived) or written to a `ingestion.run_metrics` Timescale hypertable. Grafana dashboard per source. Key SLO metric: **freshness** = `now - max(occurred_at produced)` per source, which drives staleness alerts (§3.4).

---

## 2. Per-source connector specs

> Cadence, formats, and access constraints below were verified against the live 2026 sites (see Sources at end). Where a source is closed/paid (PMD), the spec commits to a fallback rather than blocking Phase 1.

### 2.1 NIH Pakistan — IDSR weekly epidemiological bulletins

**What exists (verified):** The National Institute of Health's Field Epidemiology & Disease Surveillance Division publishes the **IDSR Weekly Epidemiological Bulletin** as a **PDF**, one per epidemiological week, on the WordPress site `nih.org.pk`. Files live under `wp-content/uploads/YYYY/MM/` with names like `Weekly_Report-39-2025.pdf`, `Weekly Report-45-2023.pdf`, `IDSR-Weekly-Report-16-2022.pdf` — **naming is inconsistent across years** (underscore vs space vs `IDSR-` prefix), the month folder varies, and URLs carry a `?v=NN` cache-buster. Content: district/province tables for priority diseases (malaria, dengue, acute diarrhoea/cholera, ILI/ARI, typhoid, measles, etc.), plus outbreak-alert narrative and field-activity sections.

**Implication:** URL is *not* reliably constructable from `(week, year)`. **Discovery must scrape the bulletin listing page**, not template a URL.

**Connector design:**
- `discover`: fetch the IDSR publications listing page; extract all PDF anchors; parse `(year, epi_week)` from link text/filename with a set of tolerant regexes; emit `DiscoveredItem(identity=f"idsr:{year}:W{week}")`. Filter by watermark. Keep a small hand-maintained `known_editions.yaml` mapping so a missed week is loud.
- `fetch`: GET the PDF (respect rate limit; single host).
- `archive_raw`: SDK → bronze (`nih_idsr/<year>/W<week>/<hash>/...pdf`).
- `parse` — **agentic parse, cached (ADR-012).** The original tiered `pdfplumber → camelot → LLM-fallback` strategy is **superseded**: these bulletins do not have a reliably extractable text layer, so the deterministic tiers were never going to be the hot path.

  ```
  parse(raw):
    1. ctx.parse_cache.get(content_hash, "llamaparse@1")     ← read-through cache
         hit  → return cached structured output  (deterministic replay, zero cost)
         miss ↓
    2. agentic parse (LlamaParse)  ── ONLY IF dim_source.access_tier ∈ {public, historical}
         │                             else: raise — on-prem parse only (ADR-012 §3)
         ▼
    3. ctx.parse_cache.put(content_hash, "llamaparse@1", output)   ← IMMUTABLE, permanent
    4. cross-check with pdfplumber/camelot where a text layer exists (disagreement = quality signal)
    5. RECONCILE TOTALS  ← the primary defence, see below
  ```

  The cached output lives beside the raw bytes and is **never overwritten**:
  ```
  chip-bronze/nih_idsr/2025/W39/sha256-ab12.../Weekly_Report-39-2025.pdf
  chip-bronze/nih_idsr/2025/W39/sha256-ab12.../parse/llamaparse@1.json
  ```
  This is what restores ADR-005's determinism guarantee (an agentic parser is *not* deterministic; a cached artifact is), caps the per-page cost at exactly one call per document forever, and collapses vendor risk to new documents only.

- `validate` — **numeric reconciliation is now mandatory, not a safety net.** This is the single most important change in this document.

  > **An agentic parser fails differently from a deterministic one.** `camelot` fails by returning *nothing*, which is loud and safe. An LLM/VLM fails by returning a **plausible, well-formed, wrong number** — which is silent, and which is exactly the corruption that destroys institutional trust in an epidemiological platform.

  Therefore every parsed bulletin must pass, before any record is produced:
  1. **Row/column totals reconcile against the printed totals in the bulletin.** A table whose district counts do not sum to its printed provincial/national total is **quarantined whole** (`reason=totals_mismatch`) — no partial records.
  2. Per-row pydantic checks: district ∈ COD-AB admin2 gazetteer (**resolved against the boundary vintage valid on the bulletin's own date** — ADR-015), disease ∈ controlled vocab, `cases >= 0`, week ∈ 1..53.
  3. Cross-check against the deterministic extractor where a text layer exists; log disagreement rate per bulletin as a quality metric.

  **Better a gap than a wrong epi number.** A quarantined bulletin is a visible, triageable backlog item; a hallucinated case count is an invisible lie that propagates into the panel, the models, the KG, and a policy brief.

- **Format-change handling:** unchanged and still essential — the `layout_signature` gate (§5) runs *before* parse, so an unrecognised bulletin layout is quarantined whole rather than fed to the parser and silently mangled.

- **Extract the FULL retrospective table, not just the current week (ADR-015).** If a bulletin reprints prior weeks' numbers, those restatements **are the vintage record** on which every forecast-skill claim in subsystem 05 depends — and they are destroyed forever if the parser only reads the current-week row. Extracting the whole table is cheap insurance; `ingestion_revision` (below) then tells us empirically whether it mattered.

- **Revision detection (ADR-015).** The normalizer's UPSERT into `fact_disease_cases` writes an `ingestion_revision` row whenever an incoming value differs from a stored one, capturing both values and both `bronze_uri`s. This is the **only** place a revision is observable — by the time the KG is built, the panel has already overwritten the evidence. ~10 lines; commits us to nothing; answers 05 OQ-3 with data instead of belief.

- **Historical backfill:** editions reach back to ~2021 (plus older FELTP/DEWS reports). A Dagster **partitioned job** (partition = epi-week, §3.2) walks all discoverable historical PDFs once, archives to bronze, agentic-parses, caches, and produces with a `backfill` run tag. **~500+ PDFs ≈ 10,000 pages** — verify whether the parser subscription bills per page or per document before planning a quota (OQ-2). Compute is *not* the constraint: a plain Python loop with a process pool finishes in well under an hour. **This backfill is the highest-value task in the project** — see §4a.

**Grain note:** IDSR is already ~district×week, i.e. very close to the canonical grain — this is the anchor source for ADR-005.

### 2.2 PMD weather (Pakistan Meteorological Department / CDPC) + global fallbacks

**What exists (verified):** PMD's **Climate Data Processing Centre** (CDPC, Karachi, est. 1988) is the official climatological archive, but data is supplied under a **paid, single-end-user licence** ("cannot be passed to a third party; saleable/onward-dissemination requires formal negotiation"), with a published **cost list** — i.e. **no open public API and a licence that conflicts with redistribution in a research platform.** `weather.gov.pk` offers regional forecast pages (scrapeable but forecast, not clean historical station series). A PMD weather API was reported *under development* (LMKT/Code for Pakistan) but is **not confirmed as an open, free, historical endpoint** as of 2026.

**Decision:** Do **not** make CDPC the Phase-1 dependency. Use **global open reanalysis/observation datasets as the primary district-level historical weather source**, and treat PMD as a *validation/enrichment* feed to be added once an MOU/licence exists.

**Recommended pragmatic mix (primary → fallback):**

| Need | Source | Why |
|---|---|---|
| **Primary: district-level daily/hourly historical T/precip/humidity, 1940–present** | **Open-Meteo Historical (Archive) API** — ERA5 0.25° (~25 km) from 1940, ERA5-Land 0.1° (~9 km) from 1950, **CC-BY 4.0, free** | Simple JSON API, no auth, redistributable with attribution, query by lat/lon = **district centroid**. Directly yields the canonical grain when centroids come from COD-AB admin2. |
| Station-based observations (ground truth where stations exist) | **NOAA GSOD / GHCN-Daily** (public domain) | Real station obs for PMD synoptic stations that report to WMO GTS; good for validating reanalysis and for long records. Sparse coverage → not a standalone district source. |
| Bulk/self-hosted reanalysis (heavy backfills, custom variables, heat indices) | **ERA5 / ERA5-Land via Copernicus CDS** (`cdsapi`) | Full control, all variables (e.g. wet-bulb for heat-health), but larger ops burden (GRIB/NetCDF, request queue). Use for research-grade backfills, not the daily feed. |
| Air quality / smog (proposal mentions urban smog) | **Open-Meteo Air-Quality API** (CAMS) | Same client, adds PM2.5/PM10 for the smog–ARI link. |

**Connector design (Open-Meteo primary):**
- `discover`: from the district gazetteer (COD-AB admin2 centroids) build one item per `(district, date-range chunk)`. For the live daily feed, discover = "yesterday for all ~160 districts".
- `fetch`: batched GET to Open-Meteo Archive API (`latitude, longitude, start_date, end_date, daily=temperature_2m_max/min,precipitation_sum,relative_humidity_2m_mean, timezone=Asia/Karachi`). Rate-limit politely; Open-Meteo has generous free non-commercial limits but we cache aggressively.
- `archive_raw`: store the raw JSON response per district-chunk in bronze (yes, even API JSON gets archived — replay parity).
- `parse/validate`: one `SourceRecord` per district×day; validate ranges (temp plausibility, precip ≥ 0, humidity 0–100). Aggregation district×day → district×**epi-week** is a **normalizer** job, not the connector (keep connectors dumb).
- **Provenance nuance:** because reanalysis is a *model estimate*, `payload` carries `source_dataset` (`ERA5`/`ERA5-Land`) and `spatial_method: "district_centroid"`; the serving app must never present reanalysis as a PMD ground observation. When PMD data arrives, it is a *separate source* (`pmd_cdpc`) and the normalizer prefers it where available.

### 2.3 NDMA / PDMA — disaster situation reports

**What exists (verified):** NDMA publishes **daily SITREPs as PDFs** at `ndma.gov.pk/sitreps`, stored under `ndma.gov.pk/storage/sitreps/<MonthYYYY>/<random-id>.pdf` — filenames are **opaque random slugs** (e.g. `KRgdunvv4Nksroa49bU7.pdf`), so URLs are *not* constructable; discovery must scrape the listing page. During monsoon there is a numbered daily series ("NDMA Monsoon 2025 Daily Situation Report No. 67", covering a fixed 24h window). NDMA also posts **DEWS** (disaster early-warning) bulletins under `/storage/dews/` and other publications under `/storage/publications/`. PDMAs (Punjab, Sindh, KP, Balochistan) publish their own sitreps on provincial sites with **heterogeneous, less predictable layouts**.

**Connector design:**
- One connector per authority (`ndma_sitrep`, `pdma_punjab`, `pdma_sindh`, …) sharing the SDK; NDMA first, PDMAs added incrementally.
- `discover`: scrape `ndma.gov.pk/sitreps` (and `/dews`) listing; extract PDF links + the visible sitrep number/date; `identity = "ndma:monsoon2025:sitrep-67"` or `"ndma:dews:2025-06-15"`.
- `fetch/archive`: as SDK.
- `parse`: SITREPs are **semi-structured PDFs** (province/district tables of casualties, houses damaged, rainfall, plus narrative). **Same agentic-parse-plus-cache path as IDSR (ADR-012, §2.1)** — read-through `parse_cache`, agentic parse on miss (permitted: NDMA sitreps are `access_tier = public`), immutable cache write, then reconciliation. Extract: event_type (flood/heatwave/drought), district, metrics (deaths, injuries, houses damaged, rainfall_mm), reporting window.
- `validate`: district ∈ gazetteer (**at the sitrep's own boundary vintage** — ADR-015); counts ≥ 0; window parseable; **totals reconcile to the printed provincial/national rollups** — same hallucination defence as §2.1, for the same reason.
- **Cadence:** Dagster schedule = **daily during monsoon (Jun–Sep), 2–3×/week off-season**, driven by a config flag; plus an on-demand trigger. Freshness SLO tightens in monsoon (§3.3).

**Structured fallbacks (strongly recommended, run in parallel):**
- **ReliefWeb API** (`api.reliefweb.int/v1/reports`) — curated Pakistan situation reports as **structured JSON with metadata** (country, disaster, format="Situation Report", dates, source, body). Note the 2025 rule: **`appname` must be pre-approved**; limits 1000 calls/day, 1000 entries/call. This is a clean, low-effort connector (`news`-like: JSON, no PDF parsing) that gives us NDMA/UN/NGO sitreps already tagged — excellent cross-check and gap-filler for NDMA PDFs.
- **HDX (`data.humdata.org`, CKAN API)** — Pakistan flood datasets, IDP/damage figures, and (critically) **COD-AB admin boundaries** used as our district gazetteer (§2.5).
Recommendation: NDMA PDF connector = authoritative national numbers; ReliefWeb connector = structured redundancy + international framing; reconcile in the normalizer.

### 2.4 News — the `naaas` connector (ADR-013)

> **This section replaces the six per-outlet scrapers (Dawn, Tribune, express.pk, Jang, BBC Urdu, Geo) in the original draft.** CHIP does not scrape news.

**What exists:** the PCN lab's **previous NRPU project — News Analytics as a Service (NAaaS)** — already owns a news collection and indexing pipeline, being scaled to multilingual with additional sources. It exposes a **query API: keywords + date range → articles.** The proposal frames CHIP as *"extending the validated NAaaS backbone"*, so consuming NAaaS is the **literal** reading of the funded design.

**What this buys, and why it is not a shortcut:**

1. **It removes the acquisition wall.** Polite scraping (~20 req/min) cannot fetch a multi-year Pakistani news archive — a million articles is ~35 days of continuous scraping, and no outlet offers a bulk download. NAaaS has already paid this cost.
2. **The relevance gate is free.** Only ~2–5% of the news firehose is health/climate relevant (03 §3.1). A keyword-driven query means CHIP **never ingests the other 95–98%** — which is what makes the GPU budget (ADR-014), the pgvector sizing, and the CHKG node count (04 §3.3) actually true. Without this gate they inflate **20–50×**.
3. **It deletes an entire legal surface** — no robots.txt posture, no per-outlet ToS analysis, no fair-dealing argument, no paywall question.
4. **It deletes six connectors** from a rotating student team's maintenance load, and six independent points of silent breakage when an outlet changes its HTML.

**Connector design (`naaas`) — the easiest connector in the project:**
- `discover`: for each `(query_term, date_window)` partition, build one item. Query terms = the CHIP **disease + hazard + climate lexicon** (from `dim_disease` / `dim_hazard_type`, including Urdu aliases). Live: rolling window, polled every 15–30 min. Backfill: date-range partitions.
- `fetch`: authenticated `GET` to the NAaaS API. **JSON in, no PDF parsing, no HTML extraction, no encoding archaeology** — NAaaS owns all of it.
- `archive_raw`: archive the NAaaS **response** to bronze (replay parity, as with every other source). Whether CHIP must *also* re-archive full article text depends on contract **C2** below.
- `parse` / `validate`: map NAaaS records to `SourceRecord`; require a stable `doc_id`, a publish timestamp, a language tag, and non-empty text.
- **Dedup:** NAaaS owns exact + near-duplicate detection at collection time. CHIP keeps only `doc_id` identity dedup. **Cross-outlet wire-story clustering** (the same PPI/APP story in Dawn *and* Tribune) stays with the NLP layer *unless NAaaS already returns a cluster id* — **ask them; if they do, 03 §6.1 stage [2] disappears entirely.** Unresolved wire copy inflates `media_mentions` and biases the project's headline feature.

#### The two contracts CHIP must obtain from NAaaS — agree these NOW, while the API is being designed

Both are cheap for NAaaS to build and expensive to retrofit.

**C1 — an unfiltered count endpoint (the media-surge denominator).**
Subsystem 05 §2.4 normalises the media signal by **district total news volume** — the entire point is to distinguish *"more disease news"* from *"more news."* A keyword-only API returns the numerator and **destroys the denominator.**

```
GET /v1/counts?district=<pcode>&date_from=&date_to=&granularity=day
  → { "district": "PK101", "date": "2026-07-08", "total_articles": 412 }
```

Without it, `media_surge_z` is uninterpretable and **the project's headline hypothesis — that news leads official surveillance — cannot be tested honestly**, because a district whose papers simply publish more would look identical to a district with an outbreak.

**C2 — stable document identity + durable, retrievable text.**
ADR-005 requires every media-derived assertion to trace to a document span; 04 §1.3 stores `char_start`/`char_end` offsets into normalized text. **Those offsets rot if the text ever changes.** NAaaS must guarantee: a **stable `doc_id`** (never reused, never mutated); a **content hash** of the normalized text; **retrievability, byte-identical, for 3+ years**; and **versioned** re-extraction (publish a new version, never mutate the old).

> **If NAaaS cannot guarantee durability, CHIP must re-archive full text into its own bronze** — more storage, but auditability is non-negotiable (ADR-005). Decide before Phase 2.

**Urdu handling** moves to NAaaS (UTF-8 end-to-end, NFC normalisation, Arabic/Urdu confusable code points — ی/ي, ہ/ه, ک/ك — RLM/LRM/ZWNJ stripping, Eastern-Arabic-Indic digit mapping). **CHIP must still validate it on arrival, not assume it**: the confusable-normalisation map is load-bearing for *district-name geo-linking* (03 §5), so CHIP re-applies its own normalisation before entity linking rather than trusting the upstream. Belt and braces — a mis-normalised ی silently breaks a district match.

**The dependency this creates, stated plainly:** CHIP's news is now only as good as NAaaS's coverage, recall, freshness, and uptime. **NAaaS's outlet list, language coverage, and freshness SLO become CHIP's.** Track them as such — a Dagster asset check on NAaaS freshness, and a documented escalation path when an outlet CHIP needs is missing. The answer to a missing outlet is **add it to NAaaS**, never "write a CHIP scraper" — that would reintroduce the two-ingestion-path entropy this ADR exists to prevent.

### 2.5 Historical / open datasets to bootstrap Phase 1

Concrete, verified candidates to seed the platform before institutional feeds arrive:

| Dataset | What it gives | Access |
|---|---|---|
| **OpenDengue** (opendengue.org; Nature Sci Data 2024; figshare V1.2) | Global dengue case counts incl. **Pakistan**, weekly/monthly, admin0–2, 1990+ (some to 1924). CC-BY. | CSV download / figshare / GitHub — a batch connector, no scraping. Pakistan subnational is thin but a real bootstrap for the dengue×week target. |
| **HDX Pakistan group** (`data.humdata.org/group/pak`) | Flood impact, IDP/damage, WHO health indicators, DHS; **COD-AB admin boundaries** (admin0–3). | CKAN API — programmatic. |
| **COD-AB Pakistan admin boundaries** (via HDX/OCHA) | **The district gazetteer** (admin2) = spatial backbone for ADR-005 + geocoding + weather centroids. | HDX download; load into PostGIS. |
| **PITB Dengue Activity Tracking System (DATS)** + **DEAG downloads** (deag.punjab.gov.pk) + **DGHS Punjab Epidemic Control Center** | Punjab dengue surveillance/activity history (36 districts, 2012+). | Web (DATS is largely operational dashboards; DEAG/DGHS publish reports/PDFs — treat like NDMA). |
| **data.gov.pk / provincial open-data** | District-wise disease and demographic tables where published. | CKAN/portal; coverage uneven — opportunistic. |
| **WHO Disease Outbreak News (DON)** | Curated Pakistan outbreak events (e.g. 2022 dengue, cholera) for event ground-truth. | Web/JSON; low volume. |
| **NOAA GSOD / GHCN, ERA5** (§2.2) | Historical weather to pair against every historical disease record. | APIs. |

These make Phase 1 fully functional on **public data only** (per the proposal's staged strategy and the "PROJECT WEAKNESSES AT LAUNCH" mitigation), while the SDK's `pmd_cdpc`/`nih_idsr_feed` authenticated variants wait behind MOUs.

---

## 3. Scheduling & orchestration (Dagster)

### 3.1 Assets vs ops/jobs — the pattern

We model each source's **bronze landing** and **raw-to-Kafka production** as **Dagster assets** (they have identity, lineage, freshness, and materialisation history), and wrap the imperative connector run in an **op-based job** where a source is fundamentally imperative (scrape loop). Practical rule:

- **Software-defined asset** per source: `raw_<source>` (materialising it = "run the connector for the current window, archive to bronze, produce to Kafka"). This gives us the Dagster asset catalog, freshness policies, and auto-materialisation.
- **Ops/jobs** for multi-step imperative flows (e.g. discover → fan-out fetch → produce) when we want visible per-step retries in the Dagster UI.

```python
# pipelines/ingestion/assets.py
from dagster import asset, AssetExecutionContext, FreshnessPolicy, RetryPolicy, Backoff

@asset(
    group_name="ingestion",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 8),   # IDSR: ~weekly + slack
    retry_policy=RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL),
    kinds={"kafka", "minio"},
)
def raw_nih_idsr(context: AssetExecutionContext, connector_ctx: RunContext) -> None:
    from connectors.nih_idsr import NihIdsrConnector
    summary = run_connector(NihIdsrConnector(), connector_ctx)
    context.add_output_metadata(summary.as_metadata())   # produced/skipped/quarantined counts
```

`connector_ctx` is a **Dagster resource** that builds the SDK `RunContext` (Kafka producer, MinIO client, Postgres dedup/watermark stores, LLM client) once and injects it — so credentials and clients are configured in one place (§6).

### 3.2 Partitioned backfills

Each historical/enumerable source declares a **partitions definition**; backfill = materialising a range of partitions, which Dagster parallelises and tracks per-partition (so a failed 2023-W14 doesn't lose 2023-W15).

```python
# pipelines/ingestion/partitions.py
from dagster import WeeklyPartitionsDefinition, MultiPartitionsDefinition, StaticPartitionsDefinition

epi_week_partitions = WeeklyPartitionsDefinition(start_date="2021-01-04")   # IDSR history
district_weather_partitions = MultiPartitionsDefinition({
    "district": StaticPartitionsDefinition(load_district_codes()),          # COD-AB admin2
    "week":     WeeklyPartitionsDefinition(start_date="1990-01-01"),
})
```
- **IDSR / NDMA / news**: weekly (or daily) time partitions → replay any historical window.
- **Weather**: `district × week` multipartition → matches the canonical grain and lets us backfill one district or one week independently.
- Backfills run with a `run_tag: backfill` so metrics/alerts distinguish them from live ingestion, and they honour the same dedup (re-running a partition is a no-op if content is unchanged).

### 3.3 Freshness policies / SLOs per source

| Source | Cadence | Freshness SLO (max lag) | Rationale |
|---|---|---|---|
| `nih_idsr` | weekly | 8 days | weekly bulletin + publish delay slack |
| `pmd_weather` (Open-Meteo) | daily | 48 h | reanalysis has ~5-day final latency; 48h for provisional |
| `ndma_sitrep` (monsoon) | daily | 30 h | daily 24h window + processing |
| `ndma_sitrep` (off-season) | 2–3×/wk | 4 days | fewer events |
| `reliefweb` | daily poll | 36 h | curated, some lag |
| `naaas` (news, ADR-013) | 15–30 min poll | 90 min | near-real-time early signal is the point |
| bootstrap datasets (OpenDengue/HDX) | monthly/ad-hoc | 45 days | slow-moving reference data |

Freshness policies are declared on the asset; Dagster shows red when an asset is overdue. **Schedules** (not sensors) drive the polled sources; a **sensor** can additionally trigger `ndma_sitrep` when a new listing entry appears, for monsoon responsiveness.

> **`naaas` freshness is a *borrowed* SLO, and must be monitored as such (ADR-013).** CHIP's news freshness cannot be better than NAaaS's own collection freshness. The asset check must distinguish **"NAaaS returned nothing because there is no news"** from **"NAaaS is stale/down"** — otherwise a silent upstream outage looks exactly like a quiet news week, and the media-lead signal degrades invisibly. Query the NAaaS count endpoint (contract C1) as the liveness probe, not the keyword query.

### 3.4 Alerting when a source goes stale

- **Freshness breach** → Dagster asset check fails → alert (Slack/email webhook via Dagster's alerting or a custom `@asset_check`). Staleness is the #1 operational risk (sources silently stop publishing / change URLs).
- **DLQ depth** and **quarantine rate** thresholds → alert (`ingestion.dead_letter` unresolved count, `quarantine` rows/day per source). A spike in quarantine = probable format change (§5).
- **Zero-discovery guard**: if `discover()` returns 0 items for a source that should have new data (e.g. news feed empty for >2 cycles), raise — this catches a changed RSS URL or a blocked scraper before freshness even trips.
- A weekly **"ingestion health" digest** asset summarises produced/skipped/dlq/quarantine per source for the standup — good for a rotating student team to see the whole surface at a glance.

```python
# pipelines/ingestion/checks.py
from dagster import asset_check, AssetCheckResult

@asset_check(asset="raw_naaas")
def naaas_is_live_not_merely_quiet(connector_ctx) -> AssetCheckResult:
    """Distinguish 'no relevant news' from 'NAaaS is down'.

    The keyword query legitimately returns zero on a quiet day. Only the
    UNFILTERED count endpoint (ADR-013 contract C1) can tell us whether the
    upstream is actually alive — which is a second, independent reason we
    need that endpoint beyond the media-surge denominator.
    """
    total = connector_ctx.naaas.total_articles(hours=3)     # unfiltered liveness probe
    matched = connector_ctx.watermark.items_since(hours=3, source="naaas")
    return AssetCheckResult(
        passed=total > 0,                                   # upstream alive?
        metadata={"naaas_total_3h": total, "chip_matched_3h": matched},
        severity="ERROR" if total == 0 else "INFO",         # 0 total => upstream outage
    )
```

---

## 4. Replay & reprocessing (bronze + Kafka retention)

The whole reason for archive-first (§1.2) and `transform_version` (§1.6) is **cheap, correct reprocessing** when a parser or normalizer improves.

**Two replay tiers:**

1. **Re-produce from bronze (most common).** Parsers improve constantly (better IDSR table adapter, better Urdu extraction). Procedure:
   - Bump the connector's `transform_version` (e.g. IDSR `2.1.0 → 2.2.0`).
   - Run the **`replay_from_bronze` Dagster job** over the target partitions/source. It lists bronze objects (by prefix/time range), re-runs **only** `parse → validate → produce` on the archived bytes (no `fetch`, no source load), and produces new envelopes carrying the new `transform_version`.
   - Downstream normalizers UPSERT at canonical grain keyed by `(district, disease, epi_week, source)`; the newer `transform_version` **supersedes** the old record. Because dedup is content+key based and normalizer writes are idempotent, replay is safe to re-run.
   ```python
   # pipelines/ingestion/replay.py
   @job
   def replay_from_bronze():
       # config: source, transform_version, time_range / partition_keys
       reparse_and_produce()   # reads MinIO, skips fetch, produces with new transform_version
   ```

2. **Full pipeline replay via Kafka retention.** For downstream changes (normalizer/enricher logic), we replay the **existing Kafka messages** rather than re-parsing. `chip.raw.*` topics are configured with **long/compacted retention** (raw topics: 90+ days time retention *or* compaction keyed by `record_key` so the latest per key is always present). A normalizer improvement is deployed with a **new consumer group** that resets to the earliest offset and rebuilds silver/gold from scratch. Bronze remains the ultimate backstop if Kafka retention has aged out.

**Versioning discipline (ADR-005):**
- `connector_version` — changes when fetch/discover behaviour changes.
- `transform_version` — changes when `parse`/`validate` output could change for the same bytes. **This is the field that gates replay.** Semver, recorded on every record, and in a `transform_versions.md` changelog with the reason and affected partitions.
- Every silver/gold row keeps `source`, `transform_version`, `raw_object_key` so you can trace any analytical number back to the exact PDF and the exact parser that produced it — the "explainable and auditable" requirement in the proposal.

**Reproducibility guarantee:** given the same bronze object and the same `transform_version`, `parse` must be deterministic. **An agentic parser is not deterministic — so the cache is what provides the guarantee, not the model** (ADR-012). `replay_from_bronze` reads `parse/<parser_id>@<version>.json`, never re-calls the parser, unless `parser_version` is deliberately bumped (which is itself a `transform_version` bump, and therefore a tracked, intentional re-derivation). This is why ADR-012's cache is not an optimisation: **it is the mechanism that keeps ADR-005 true.**

---

## 4a. The historical backfill is the highest-value task in the project — build it first

This is a **sequencing decision**, and it is the most important one in this document.

**It is not a big-data problem.** The numbers, honestly:

| Backfill | Volume | Compute |
|---|---|---|
| NIH health (~500+ PDFs, ~10,000 pages) | ~1.5 GB | Bounded by the **parser API**, not by CPU. A process pool finishes the local work in well under an hour. |
| Weather (160 districts × 15 yr) | ~876k district-days → 125k district-epiweek rows; a few hundred MB of JSON | **~500 API calls.** Minutes. |
| NDMA/PDMA sitreps | Same order as IDSR | Same. |
| News | **Solved by ADR-013** — NAaaS already holds the corpus; CHIP queries it by keyword + date range | No scraping, no acquisition wall |

**No Spark cluster. No distributed anything.** ADR-002 permits Spark for backfills, and the news *enrichment* backfill (03 §6.5) genuinely uses it — but nothing here needs a cluster.

**Why it must come before the live pipeline:**

1. **It is the only training data that will ever exist.** The binding constraint on this entire project is *the depth of the disease record* — not compute, not throughput, not GPUs. Each additional year of IDSR bulletins is roughly a **20% increase** in the panel that subsystem 05's models train on.
2. **It is the vintage record** (ADR-015). Every forecast-skill claim in subsystem 05 rests on knowing what was knowable when. That information exists *only* in the archive of dated bulletins, and only if the parser reads their full retrospective tables.
3. **It is the best integration test that exists.** It drives a large, varied, layout-drifting corpus through parse → validate → quarantine → normalize → panel → KG. Every failure mode the live pipeline will ever hit — a new layout, a renamed district, a boundary split, a reconciliation mismatch — appears here first, in bulk, where it is cheap to find.
4. **The live pipeline is trivially small.** ~200–1,500 messages/day across three of four sources. **Building it first validates nothing.** If the backfill works, live works. The converse is false.

**Backfill = the same code path**, run over historical partitions (§3.2) with a `backfill` run tag. Not a second pipeline. A second pipeline would mean two parsers, two sets of bugs, and history parsed by code that never runs again — guaranteed divergence.

---

## 5. Schema-drift quarantine flow

Sources *will* change format (new IDSR layout, NDMA renames a column, an outlet restructures HTML). The system must **fail loud, lose nothing, and keep running for unaffected records.**

```
parse ─▶ validate ─┬─ pass ─▶ produce to chip.raw.<source>
                   └─ fail ─▶ QUARANTINE:
                               • raw bytes already safe in bronze (archive-first)
                               • record + validation error + provenance -> chip.quarantine.<source>
                               • row in ingestion.quarantine (payload, error, layout_signature)
                               • metric: quarantine_rate{source} ++   -> alert if over threshold
```

**Layout-signature gate (for PDF sources):** before per-row validation, the parser computes a `layout_signature` (hash of detected headers/column set). Unknown signature → the **entire document** is quarantined with `reason=unknown_layout` and **no partial/garbage records are produced** (better a gap than wrong epi numbers). Known-but-row-invalid → only offending rows quarantined.

**Triage loop (documented runbook):**
1. Alert fires (quarantine spike / unknown layout).
2. Engineer pulls the exact bronze object from `raw_object_key` (in the quarantine row) — no need to re-download.
3. Add/adjust a layout adapter or validation rule; bump `transform_version`.
4. Run `replay_from_bronze` for the affected partitions (§4) → quarantined items re-flow and clear.
5. Mark `ingestion.quarantine.resolved = true`.

**Isolation guarantee:** quarantine is **per-record/per-document**, so a Dawn HTML change never blocks IDSR ingestion, and one malformed sitrep doesn't stop the day's other sitreps. This directly serves the proposal's "schema-flexible and modular, enabling rapid adaptation to revised formats" mitigation.

---

## 6. Monorepo layout, config, and secrets

### 6.1 Directory layout

```
libs/
  chip_connectors/            # the SDK (§1)
    base.py                   # Connector ABC, DiscoveredItem, RawArtifact, SourceRecord, Provenance
    runner.py                 # run_connector() driver
    http.py                   # session, RetryPolicy, RateLimiter, robots.txt guard
    bronze.py                 # MinIO archival (content-addressed)
    kafka.py                  # Envelope producer
    dedup.py                  # seen_items / seen_content / watermarks (Postgres-backed)
    dlq.py  quarantine.py     # dead-letter + quarantine sinks
    metrics.py  logging.py    # structlog + Prometheus/Timescale
    parse_cache.py            # ADR-012: read-through cache of agentic-parse output in bronze;
                              #   enforces the access_tier gate (no cloud parse for mou-pending/restricted)
    extract/                  # agentic.py (LlamaParse client), pdf.py (pdfplumber/camelot CROSS-CHECK only),
                              #   reconcile.py (row/column total reconciliation — the hallucination defence),
                              #   urdu.py (NFC + confusable normalisation — still applied on NAaaS text)
  chip_schemas/               # pydantic models + JSON Schema for Envelope and per-source payloads
  chip_geo/                   # district gazetteer (COD-AB admin2, SCD-2 + lineage per ADR-015)

connectors/                   # one package per source; each is thin (uses the SDK)
  nih_idsr/     __init__.py connector.py layouts/ config.yaml
  pmd_weather/  connector.py providers/{openmeteo.py,gsod.py,era5.py} config.yaml
  ndma_sitrep/  connector.py config.yaml
  reliefweb/    connector.py config.yaml
  naaas/        connector.py config.yaml     # ADR-013: news. REPLACES news_dawn, news_tribune,
                                             #   news_express_pk, news_jang, news_bbc_urdu, news_geo.
  bootstrap/    opendengue.py hdx.py punjab_dats.py    # batch/one-shot loaders
  README.md                   # "how to add a connector in a day" guide

pipelines/
  ingestion/                  # Dagster: assets.py partitions.py schedules.py checks.py replay.py resources.py
infra/
  docker/                     # connector image, dagster image
  compose/                    # kafka, minio, postgres, neo4j, dagster (ADR-004)
  config/                     # env-specific overlays (dev/lab)
docs/
  scraping-ethics.md  transform_versions.md  runbooks/quarantine.md
```

**"Add a connector in a day" contract:** copy a `connectors/<x>/` folder, implement 4 methods (`discover/fetch/parse/validate`), declare a `config.yaml`, register a Dagster asset. Everything else (archival, dedup, retry, DLQ, metrics, provenance) is inherited.

### 6.2 Config management

Per-connector `config.yaml`, layered with an env overlay, loaded into a typed pydantic settings object:

```yaml
# connectors/nih_idsr/config.yaml
source: nih_idsr
access_tier: public                    # ADR-012: gates whether cloud parse is permitted AT ALL
kafka_topic: chip.health.nih_idsr.disease_case_report.v1   # ADR-007 naming
connector_version: "1.4.0"
transform_version: "2.2.0"
discovery:
  listing_url: "https://www.nih.org.pk/publications/"     # scraped, URLs not templatable
  filename_patterns: ["Weekly[_ ]Report-(\\d+)-(\\d{4})", "IDSR-Weekly-Report-(\\d+)-(\\d{4})"]
fetch:
  rate_limit: { requests_per_minute: 20, max_concurrency: 2, min_interval_ms: 500 }
  respect_robots: true
  user_agent: "CHIP-Research-Bot (NUCES/FAST; contact: pcn@nu.edu.pk)"
parse:                                 # ADR-012 — agentic primary, cached, reconciled
  primary: llamaparse                  # agentic; the ONLY path that works on these documents
  parser_version: "llamaparse@1"       # part of the bronze parse-cache key; bump = re-parse
  cache: bronze                        # read-through; a cached parse is NEVER re-billed or re-called
  cross_check: [pdfplumber]            # demoted to validator; disagreement = quality metric
  extract_full_retrospective_table: true   # ADR-015 — cheap insurance for the vintage record
validate:
  reconcile_totals: true               # MANDATORY. A VLM hallucinates plausible numbers; totals catch it.
  on_totals_mismatch: quarantine_document   # whole bulletin. Better a gap than a wrong epi number.
schedule: { cron: "0 6 * * 2", timezone: "Asia/Karachi" }   # Tuesdays 06:00 PKT
freshness_max_lag_hours: 192
```

```yaml
# connectors/pmd_weather/config.yaml (fallback-first)
source: pmd_weather
access_tier: public                    # Open-Meteo is CC-BY 4.0
providers:
  primary: openmeteo_archive     # ERA5 / ERA5-Land, CC-BY 4.0, no auth
  validation: noaa_gsod          # station cross-check
  heavy_backfill: era5_cds       # cdsapi, credentialed
grain: { spatial: district_centroid, temporal: daily }   # -> normalizer aggregates to epi-week
variables: [temperature_2m_max, temperature_2m_min, precipitation_sum, relative_humidity_2m_mean]
# NOTE: weather has NO extraction problem. No parser, no layout drift, no OCR, no agentic parse.
#       ~500 API calls covers 160 districts x 15 years. It is the easiest source in the project.
```

```yaml
# connectors/naaas/config.yaml  (ADR-013 — replaces six scraper configs)
source: naaas
access_tier: public
kafka_topic: chip.media.naaas.article.v1
api:
  base_url: "${NAAAS_API_URL}"
  auth: bearer                          # token via Docker secret, never in git
query:
  # the relevance gate — CHIP never sees the other 95-98% of the firehose
  lexicon_from: [dim_disease, dim_hazard_type]   # incl. Urdu aliases
  languages: [en, ur]
  poll_window_minutes: 30
counts_endpoint: "/v1/counts"           # CONTRACT C1 — the media-surge denominator AND the
                                        #   liveness probe (zero matches != upstream down)
doc_durability:                         # CONTRACT C2
  require_stable_doc_id: true
  require_content_hash: true
  # if NAaaS cannot guarantee 3-yr byte-identical retrievability, flip this and CHIP
  # re-archives full text into its own bronze. Auditability (ADR-005) is not negotiable.
  rearchive_full_text: false
freshness_max_lag_minutes: 90
```

### 6.3 Secrets (for future authenticated institutional feeds)

- **Nothing secret in git.** Config references secrets by name; values come from the environment / a secrets file mounted into the container.
- Phase 1 needs almost none (public sources). Where creds exist (Copernicus CDS `cdsapi` key, ReliefWeb `appname`, future PMD/NIH portal logins), store them as **Docker secrets / `.env` excluded from VCS**, injected via the Dagster `connector_ctx` resource. A single `infra/config/secrets.example.env` documents required keys.
- Design the SDK so an **authenticated variant is a subclass**: e.g. `PmdCdpcConnector(AuthenticatedConnector)` adds a login/token step in `fetch` but reuses the entire lifecycle — so when the MOU lands, we swap the provider in config, not rewrite the pipeline. This realises the proposal's "schema-flexible and modular… rapid adaptation once institutional feeds become available."
- For self-hosting (ADR-004), a lightweight secrets store (Docker secrets now; HashiCorp Vault only if the lab standardises on it later) — no managed cloud KMS.

---

## 7. Open questions

### Closed since the 2026-07-13 review

| Was | Now |
|---|---|
| ~~OQ-3 District gazetteer / boundary versioning~~ | **Closed by ADR-015.** Subsystem 01 §1.2/§1.4 already specified it (SCD-2 `dim_location` + `location_lineage` with `area_fraction`). Data-model (01) owns it; 02/03/04 are consumers. Population-weighted apportionment; every mart declares an `analysis_vintage`. |
| ~~OQ-7 News legal posture~~ | **Closed by ADR-013.** CHIP does not scrape. The entire robots.txt / ToS / fair-dealing / paywall surface belongs to NAaaS. |
| ~~OQ-9 Backfill compute (when does it justify Spark?)~~ | **Closed: never, for ingestion.** See §4a — the health/weather/disaster backfills are hours of local work at most. Spark's only backfill role is the *news NLP enrichment* pass (03 §6.5), which is real. |
| ~~OQ-10 LLM determinism for replay~~ | **Closed by ADR-012.** Every parse result is cached immutably in bronze, keyed `(content_hash, parser_id@version)`. Replay reads the cache and never re-calls the model. Determinism restored by construction; the parser bill is capped at one call per document forever. |

### Still open

1. **PMD licensing path.** Is a CDPC data-sharing MOU realistically obtainable, and can its single-end-user licence coexist with a research platform? If not, reanalysis is *permanently* primary and PMD is a validation feed only. (Blocks how we label "official" weather.)
2. **Parser billing: per page or per document?** ⚠️ **Verify before committing to a quota.** Agentic parsers typically bill **per page**. ~500 bulletins × 15–20 pages ≈ **10,000 pages**, not 500 documents — a 20× difference against a plan built on a document count. Cheap either way, but get the number right. (ADR-012)
3. **NAaaS contract C1 — the unfiltered count endpoint.** Needed for *two* independent reasons: the media-surge denominator (05 §2.4) and upstream liveness detection (§3.4). **Agree it now, while the NAaaS API is still being designed.** (ADR-013)
4. **NAaaS contract C2 — document durability.** Can NAaaS guarantee stable `doc_id`s and byte-identical retrievable text for 3+ years? If not, CHIP must re-archive full text into its own bronze (more storage, but ADR-005 auditability is non-negotiable). (ADR-013)
5. **Does NAaaS return a wire-story cluster id?** If yes, 03 §6.1 stage [2] (cross-outlet near-dup) disappears entirely. If no, CHIP owns it — and unresolved wire copy (the same PPI/APP story in Dawn *and* Tribune) directly inflates `media_mentions`, biasing the project's headline feature.
6. **IDSR machine-readable feed.** Will NIH provide CSV/API under MOU? If yes, §2.1's parsing complexity retires wholesale. If no, budget the agentic parse indefinitely — and **fix a go/no-go accuracy threshold** for the reconciliation checks before trusting parsed epi numbers in a policy brief.
7. **Urdu geocoding accuracy.** How well do Urdu district mentions resolve to admin2 after confusable normalisation? Needs a labelled eval set before news-derived spatial signals are trusted.
8. **ReliefWeb `appname` approval** — redundancy layer, or timely enough to reduce reliance on NDMA PDF parsing?
9. **PDMA coverage.** Build all four in Phase 1, or start with Punjab + Sindh and rely on NDMA national rollups elsewhere?
10. **Kafka retention split.** 90-day retention + permanent bronze (the current default), or compact indefinitely? Bronze is the system of record either way, so this is a replay-convenience question, not a durability one.

---

### Sources (verified 2026-07-10; news sources superseded by ADR-013)

- NIH IDSR weekly bulletins (PDFs, `nih.org.pk/wp-content/uploads/...`): [Weekly_Report-39-2025](https://nih.org.pk/wp-content/uploads/2025/11/Weekly_Report-39-2025.pdf), [IDSR-Weekly-Report-16-2022](https://www.nih.org.pk/wp-content/uploads/2022/05/IDSR-Weekly-Report-16-2022.pdf)
- PMD / CDPC data licence & cost: [cdpc.pmd.gov.pk](https://cdpc.pmd.gov.pk/), [CDPC cost list](http://www.pmd.gov.pk/cdpc/cost.htm), [PMD weather API (LMKT/Code for Pakistan)](https://lmkt.com/lmkt-develop-weather-api-mobile-application-pakistan-meteorological-department/)
- Open-Meteo historical/archive (ERA5, CC-BY 4.0): [Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
- NDMA SITREPs (PDF, `ndma.gov.pk/sitreps`, opaque filenames): [NDMA sitreps](https://www.ndma.gov.pk/sitreps)
- ReliefWeb API (appname, limits, filters): [apidoc.reliefweb.int](https://apidoc.reliefweb.int/), [API help](https://reliefweb.int/help/api)
- HDX Pakistan group + COD-AB: [data.humdata.org/group/pak](https://data.humdata.org/group/pak)
- OpenDengue (global dengue counts incl. Pakistan, CC-BY): [opendengue.org/data](https://opendengue.org/data.html), [Nature Sci Data 2024](https://www.nature.com/articles/s41597-024-03120-7)
- Punjab dengue: [PITB DATS](https://pitb.gov.pk/dats), [DEAG downloads](https://deag.punjab.gov.pk/download_links), [DGHS Epidemic Control Center](https://dghs.punjab.gov.pk/epidemic_control_center)
- News RSS: [Dawn](https://www.dawn.com/), [Express Tribune RSS](https://tribune.com.pk/rss), [Pakistan RSS directory](https://rss.feedspot.com/pakistan_news_rss_feeds/)
