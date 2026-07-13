# ADR-013: News Arrives via the NAaaS API, Not Per-Outlet Scrapers

- **Status:** **Accepted** (2026-07-13)
- **Supersedes:** subsystem 02 §2.4 (per-outlet RSS/HTML scraper connectors for Dawn, Tribune, express.pk, Jang, BBC Urdu, Geo) and the scraping-ethics posture built around them.
- **Context:** The PCN research group's **previous NRPU project, News Analytics as a Service (NAaaS)**, already owns a news collection and indexing pipeline. It is being scaled to multilingual with additional sources. It will expose a **query API: keywords + date range → articles.** The proposal itself frames CHIP as *"extending the validated NAaaS backbone"* — so consuming NAaaS is the faithful reading of the funded design, not a deviation.

## Decision

**News enters CHIP through one connector (`naaas`) that queries the NAaaS API.** CHIP does not scrape Dawn, Tribune, Jang, express.pk, BBC Urdu, or any other outlet directly. Outlet coverage, RSS discovery, HTML extraction, Urdu encoding normalisation, robots.txt compliance, and rate limiting all become **NAaaS's responsibility**, where they already live.

The connector queries NAaaS on the CHIP **disease + climate + hazard lexicon** (the controlled vocabularies in `dim_disease` / `dim_hazard_type`, plus their Urdu aliases), on a rolling window for live ingest and by date-range partition for backfill.

## Why this is strictly better

1. **It eliminates the acquisition wall.** Polite scraping at ~20 req/min cannot fetch a multi-year archive of Pakistani news — a million articles would take ~35 days of continuous scraping, and no outlet offers a bulk archive. NAaaS has already paid this cost.
2. **The relevance gate comes for free.** Only ~2–5% of the news firehose is health/climate relevant (03 §3.1). A keyword-driven API means CHIP **never ingests the other 95–98%.** This was the single largest sizing defect in the pre-2026-07-13 architecture: without a gate, every cricket article was being NER'd, RE'd, geo-linked, embedded into pgvector, and turned into a `:Document` node in Neo4j. The gate reduces GPU cost, pgvector volume, and CHKG node count by **20–50×**, and is what makes the sizing in 04 §3.3 and 07 §1.1 actually true.
3. **It removes an entire legal surface.** No CHIP-operated scraper means no robots.txt posture, no per-outlet ToS analysis, no fair-dealing argument to defend, no paywall question.
4. **It removes six connectors** from a rotating student team's maintenance load, and six independent points of silent breakage when an outlet changes its HTML.

## The two contracts CHIP must obtain from NAaaS — non-negotiable

These are **hard requirements on the NAaaS API design**, and they must be agreed *now*, while that API is still being built. Both are cheap for NAaaS and expensive to retrofit.

### C1. An unfiltered count endpoint (the media-surge denominator)

Subsystem 05 §2.4 normalises the media signal by **district total news volume** — the whole point is to distinguish *"more disease news"* from *"more news."* A keyword-only API returns the numerator and destroys the denominator.

```
GET /v1/counts?district=<pcode>&date_from=&date_to=&granularity=day
  → { "district": "PK101", "date": "2026-07-08", "total_articles": 412 }
```

Unfiltered total article volume, per district (or per outlet, with CHIP doing the geo-attribution), per day. It is a count query against an index NAaaS already maintains. **Without it, `media_surge_z` is uninterpretable and the project's headline hypothesis — that news leads surveillance — cannot be tested honestly**, because a district whose newspapers simply publish more would look like a district with an outbreak.

### C2. Stable document identity + durable retrievable text

ADR-005 requires every media-derived assertion to trace to a document span, and 04 §1.3 stores `char_start`/`char_end` offsets into normalized document text. Those offsets **rot** if the document text ever changes.

NAaaS must guarantee:
- a **stable `doc_id`** that never changes or gets reused;
- a **content hash** of the normalized text;
- the document remains **retrievable and byte-identical** for the life of the project (3+ years);
- the **normalized text is versioned** — if NAaaS changes its extraction, it publishes a new version rather than mutating the old.

CHIP stores the `doc_id` + content hash + URI as its provenance pointer. **If NAaaS cannot guarantee durability, CHIP must re-archive the full text into its own bronze** (falling back to the archive-first rule) — which is more storage but preserves auditability. Decide this before Phase 2.

## Consequences

- **02 §2.4 is rewritten**: six scraper connectors → one API connector. The connector is one of the *easiest* in the project (JSON in, no PDF parsing, no HTML extraction).
- **There is no longer a genuinely continuous stream anywhere in CHIP.** News now arrives by scheduled API pull (every 15–30 min for live signal), structurally identical to the other three sources. This further weakens the engineering case for Spark Structured Streaming — see the honesty amendment in ADR-002. Spark remains (deliverable + unified batch/stream code for the historical enrichment), but nobody should claim CHIP has a real-time stream.
- **Near-dup ownership changes.** The duplicate near-dup implementations (02 §2.4 SimHash vs 03 §6.1 MinHash/LSH) collapse: **NAaaS owns exact/near-duplicate detection at collection time**; CHIP's NLP layer owns only **cross-outlet story clustering** if NAaaS does not already provide a wire-story cluster id. **Ask NAaaS whether it does** — wire copy (the same PPI/APP story in Dawn *and* Tribune) inflates `media_mentions` and directly biases the headline feature.
- **Bronze shrinks dramatically.** CHIP archives only the retrieved (relevant) subset, not the firehose — which is what makes the corrected lifetime storage figure in 00 §1 achievable.
- **A dependency risk is created:** CHIP's news is now only as good as NAaaS's coverage, recall, and uptime. NAaaS's outlet list, language coverage, and freshness SLO become **CHIP's** outlet list, language coverage, and freshness SLO. Track them as such, with a Dagster asset check on NAaaS freshness.

## Alternatives rejected

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **Consume the NAaaS API (chosen)** | Reuses funded lab infrastructure (as the proposal describes); relevance gate for free; kills the acquisition wall; removes 6 connectors and the entire scraping-legal surface | Hard dependency on a sibling project; needs the C1/C2 contracts; CHIP inherits NAaaS's coverage limits | **Chosen** |
| CHIP runs its own per-outlet scrapers (the previous design) | Full control of coverage and extraction | Duplicates a system the lab already owns; ~35 days to backfill one year politely; 6 connectors to maintain under student turnover; a legal surface to defend | Rejected |
| Both (NAaaS primary, CHIP scrapers for gap outlets) | Coverage insurance | Two ingestion paths, two dedup strategies, two provenance shapes — the exact entropy ADR-001/002 exist to prevent | Rejected. If an outlet is missing, **add it to NAaaS**, which is the point of NAaaS. |
