# Climate–Health Intelligence Platform — Architecture Diagram Guide

**A plain-language walkthrough of the system diagram. No prior reading required.**

> **Interactive diagram:** https://excalidraw.com/#json=JBOZ8anilKwOascorXceI,JZKuet9EYwMpb4JbDI7UtQ

This guide explains every block in the architecture diagram in ordinary language, then points to the
detailed design document behind it. The diagram deliberately avoids technical shorthand so it stands on
its own; this guide adds the "what it really is" and "where to read more" for each block.

---

## The one-sentence version

The platform gathers four very different kinds of information — disease reports, weather, disaster
reports, and news — cleans them so they all describe **the same district in the same week**, then uses
that harmonized data to forecast health risk and raise early-warning alerts that always come with the
evidence behind them.

**Why this shape?** The hard problem here is *integration and trust*, not data volume (the whole system
holds only a few terabytes over its lifetime). Every choice optimizes for correctness, auditability, and
being maintainable by a rotating team of students.
See [`00-ARCHITECTURE-OVERVIEW.md`](00-ARCHITECTURE-OVERVIEW.md) §1.

---

## Block-by-block

### 1 · Data Sources
The four families of information the platform draws on. They arrive on very different schedules and in
very different formats — that mismatch is the core challenge everything downstream exists to solve.

| Block on diagram | What it actually is | Read more |
|---|---|---|
| **Disease Surveillance Bulletins** | Weekly disease case-count reports published as PDF documents by the national health institute. | [`02-ingestion-connectors.md`](subsystems/02-ingestion-connectors.md) §2; parsing in [ADR-012](adr/012-document-parsing.md) |
| **Climate & Weather Records** | Historical climate reconstructions plus forecasts, pulled from open weather data services. | [`02-ingestion-connectors.md`](subsystems/02-ingestion-connectors.md) §2 |
| **Disaster Situation Reports** | Incident and situation reports from national and provincial disaster-management authorities. | [`02-ingestion-connectors.md`](subsystems/02-ingestion-connectors.md) §2 |
| **Relevance-Filtered News Feed** | Multilingual news, already screened for topical relevance by the lab's own news service before it reaches us — this pre-filter is what keeps the whole system small. | [ADR-013](adr/013-news-via-naaas.md) |

Nothing is pushed to the platform — it polls every source on a schedule.

### 2 · Collection
**Data Collection Connectors.** One small scheduled program per source, all following the same five
steps: find new items → download them → **save an untouched original copy first** → read the contents →
check the result is valid → publish it onward. Saving the original before anything else is a deliberate
safety rule: if reading a document later fails (formats *do* change), the raw bytes are already safe.
See [`02-ingestion-connectors.md`](subsystems/02-ingestion-connectors.md) §1 and
[`00-ARCHITECTURE-OVERVIEW.md`](00-ARCHITECTURE-OVERVIEW.md) §2.1.

### 3 · Permanent Archive + Message Backbone
The collector writes to two places, for two different purposes.

- **Permanent Raw Archive** — an immutable store of every original file, kept forever. This is the
  system's **single source of truth**: everything else can be rebuilt from it. If a better document
  reader is built next year, it can re-process every historical document from here.
  See [ADR-003](adr/003-storage-strategy.md), [`01-data-model-and-schemas.md`](subsystems/01-data-model-and-schemas.md) §4.
- **Message Backbone** — the delivery channel that carries cleaned, validated records to the rest of the
  system on a separate stream per source. It keeps only recent history (roughly three months) and exists
  to *move* data, not to store it long-term.
  See [ADR-002](adr/002-kafka-spark-scope.md), [ADR-007](adr/007-kafka-wire-contract.md).

The dashed **replay** arrow shows that any historical file in the archive can be re-fed into the pipeline
at any time to regenerate everything downstream. See [`02-ingestion-connectors.md`](subsystems/02-ingestion-connectors.md) §4.

### 4 · Harmonize & Understand
Two parallel processing stages that turn raw records into comparable, meaningful data.

- **Harmonizers** — the alignment step. They translate inconsistent place names into one standard set of
  districts, and translate every date into a standard "epidemiological week," so a disease report, a
  weather record, and a disaster report can all be lined up against the same place and time.
  See [ADR-005](adr/005-canonical-grain-and-provenance.md), [ADR-006](adr/006-canonical-spatial-key.md),
  [ADR-008](adr/008-epiweek-convention.md), [`01-data-model-and-schemas.md`](subsystems/01-data-model-and-schemas.md) §1–2.
- **News Language Understanding** — the text-analysis step for news. It detects the language, pulls out
  the important entities and the relationships between them, resolves when events happened, links places
  to districts, and derives "media signals" (early hints from what news is reporting).
  See [`03-nlp-pipeline.md`](subsystems/03-nlp-pipeline.md), embedding model in [ADR-011](adr/011-embedding-model.md).

### 5 · Central Data Store
**Central Analytical Database.** The single home for all harmonized data: geographic boundaries, the
time-series measurements, and searchable representations of text. It holds cleaned per-source tables and,
crucially, **one unified panel indexed by district and week** that every downstream analysis reads from.
This unified table is what makes the whole platform coherent.
See [ADR-003](adr/003-storage-strategy.md), [`01-data-model-and-schemas.md`](subsystems/01-data-model-and-schemas.md) §5.

### 6 · Analysis · Knowledge Graph · Reasoning
Two branches consume the central data store.

- **Analytics & Forecasting** — the modeling branch. It runs baseline risk models, produces forecasts,
  watches for unusual outbreak patterns, and generates **candidate** early-warning alerts that a human
  reviews before anything is published.
  See [`05-analytics-forecasting-alerting.md`](subsystems/05-analytics-forecasting-alerting.md).
- **Knowledge Graph Builder** — turns findings into a web of **evidence-backed facts**: every stated
  relationship is tied to the source that supports it.
  See [`04-knowledge-graph-rag.md`](subsystems/04-knowledge-graph-rag.md) §1–2, [ADR-010](adr/010-kg-consumption-contract.md).
- **Climate–Health Knowledge Graph** — the resulting network of facts and their evidence, which "why"
  questions can be traced through.
  See [`04-knowledge-graph-rag.md`](subsystems/04-knowledge-graph-rag.md) §3.
- **Cited-Summary Assistant** — a self-hosted language model that answers questions in plain language and
  **always shows the sources** it drew from, so answers can be trusted and checked.
  See [`04-knowledge-graph-rag.md`](subsystems/04-knowledge-graph-rag.md) §6, GPU budget in [ADR-014](adr/014-gpu-allocation.md).

### 7 · Serving
**Serving Application.** The single secure application that exposes everything to users: indicators,
forecasts, alerts, a knowledge-graph explorer, cited summaries, exportable policy briefs, plus sign-in
and audit logging so every access is accountable. It is built as one well-organized application rather
than many tiny services, to stay maintainable.
See [ADR-001](adr/001-service-topology.md), [`06-serving-dashboard.md`](subsystems/06-serving-dashboard.md) §1–2, §5.

### 8 · Users
**Stakeholder Dashboard.** The screen the institutions actually use: district maps, epidemic curves laid
over climate conditions, and an alert center where every alert carries its supporting evidence.
Available in English and Urdu, for health, disaster-management, and climate-policy stakeholders.
See [`06-serving-dashboard.md`](subsystems/06-serving-dashboard.md) §3.

---

## How to read the flow
Top to bottom is the life of a piece of data: **a source publishes → a connector collects and archives it
→ it travels the backbone → it is harmonized and understood → it lands in the central store → analysis
and the knowledge graph consume it → the serving application exposes the results → users see them on the
dashboard.** The one arrow that runs *backward* — replay — is the safety net that lets the whole chain be
rebuilt from the permanent archive.

---

### Where every block comes from (index)
- Overall shape & rationale: [`00-ARCHITECTURE-OVERVIEW.md`](00-ARCHITECTURE-OVERVIEW.md)
- Data model, districts, weeks, storage zones: [`01-data-model-and-schemas.md`](subsystems/01-data-model-and-schemas.md)
- Sources & collection: [`02-ingestion-connectors.md`](subsystems/02-ingestion-connectors.md)
- News language understanding: [`03-nlp-pipeline.md`](subsystems/03-nlp-pipeline.md)
- Knowledge graph & cited summaries: [`04-knowledge-graph-rag.md`](subsystems/04-knowledge-graph-rag.md)
- Analytics, forecasting & alerts: [`05-analytics-forecasting-alerting.md`](subsystems/05-analytics-forecasting-alerting.md)
- Serving application & dashboard: [`06-serving-dashboard.md`](subsystems/06-serving-dashboard.md)
- Infrastructure & operations: [`07-infrastructure-operations.md`](subsystems/07-infrastructure-operations.md)
- Key decisions: [`adr/`](adr/) (numbered decision records)
