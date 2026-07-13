# ADR-012: Document Parsing — Agentic Parse, Cached Immutably to Bronze

- **Status:** **Accepted** (2026-07-13)
- **Supersedes:** the `pdfplumber → camelot → LLM-fallback` extraction tiering in subsystem 02 §2.1 and §2.3.
- **Context:** The NIH IDSR bulletins and NDMA SITREPs are **not** clean digital-text tables. Empirically they require an **agentic/VLM parse** (LlamaParse-class) to extract reliably. The previous design assumed a text layer and a deterministic table extractor as the primary path; that assumption is false, which invalidates the primary and secondary tiers of the old strategy.

## Decision

**1. Agentic parse is the primary path for institutional PDFs.** Use a hosted agentic parser (LlamaParse) for the historical backfill and for the live weekly/daily PDF sources. Deterministic extractors (`pdfplumber`/`camelot`) are demoted from "primary" to "cheap pre-check and cross-validator" — they still run, and a disagreement between them and the agentic parse is a **quality signal**, not a failure.

**2. Every parse result is cached immutably in bronze, as a first-class artifact.** This is the load-bearing half of the decision.

```
s3://chip-bronze/nih_idsr/2025/W39/<sha256>/report.pdf              ← raw bytes (as before)
s3://chip-bronze/nih_idsr/2025/W39/<sha256>/parse/llamaparse@1.json ← parse output, immutable
s3://chip-bronze/nih_idsr/2025/W39/<sha256>/parse/pdfplumber@1.json ← cross-check output
```

The cached parse output is keyed by `(content_hash, parser_id@parser_version)` and is **never overwritten**. `replay_from_bronze` (02 §4) reads the *cached parse*, not the PDF, unless `parser_version` is deliberately bumped.

**3. `dim_source.access_tier` gates which parser may touch which source.** Enforced in the connector SDK, not by convention:

| `access_tier` | Meaning | Agentic cloud parse permitted? |
|---|---|---|
| `public` | Published on a public website (IDSR bulletins, NDMA sitreps, HDX) | **Yes** |
| `historical` | Public archival datasets (OpenDengue, ReliefWeb) | **Yes** |
| `mou-pending` | Awaiting a data-sharing agreement | **No** — on-prem parse only |
| `restricted` | Under a DSA/MOU, or any facility/patient-level data | **No** — on-prem parse only, hard-blocked in the SDK |

The SDK raises at runtime if a connector whose source is `mou-pending`/`restricted` attempts a cloud parse call. This is a **data-governance control**, not a style preference: routing NIH-under-MOU or hospital data to a third-party US SaaS would breach the proposal's own ethics commitments (formal data-sharing agreements, approved administrative resolutions).

**4. The in-house parser is an evaluation track, not a prerequisite.** The cached agentic outputs become the **ground-truth training/eval set** for a local parser (Docling / Marker / MinerU / a local VLM). If a local path reaches parity on CHIP's specific document classes, we switch and drop the dependency. If it does not, we lose nothing.

## Why cache-to-bronze is non-negotiable

Three problems, one mechanism:

| Problem | How the cache solves it |
|---|---|
| **Non-determinism breaks ADR-005.** ADR-005 requires that, given the same bronze object and the same `transform_version`, `parse` be deterministic. An agentic LLM parser is not. | Replay reads the **cached output**, not the model. Determinism is restored by construction. The model is called exactly once per `(document, parser_version)`, ever. |
| **Cost.** Agentic parse bills per **page**, not per document. ~500 IDSR bulletins × 15–20 pages ≈ **10,000 pages** — verify the actual quota before planning against a document count. | Pay once per page, forever. Re-parsing is free because it never happens. |
| **Vendor risk over a 3-year project.** LlamaParse could reprice, degrade, or disappear. | The cached outputs are permanent and ours. Vendor risk collapses to *new documents only* (~400/year), and the local-parser eval track (decision 4) is the exit. |

## Alternatives rejected

| Route | Pros | Cons | Verdict |
|---|---|---|---|
| **Agentic parse + immutable bronze cache (chosen)** | Actually works on these documents; deterministic replay; pay-once; vendor-swappable; produces its own eval set for an in-house replacement | A cloud dependency in the ingestion path for public sources; per-page cost; must enforce the `access_tier` gate | **Chosen** |
| Build an in-house parser to LlamaParse quality first | No vendor dependency; fully on-prem from day one | Blocks the backfill — which is the highest-value task in the project (see 02 §4a) — on a research problem of unknown duration. Trades weeks of delay for ~$50. | Rejected as a *prerequisite*; retained as a parallel eval track |
| Keep `pdfplumber`/`camelot` as primary, agentic as fallback | No cloud call in the common case | Empirically false premise: the deterministic tiers do not work on these documents, so the "fallback" would be the hot path anyway, without the caching discipline | Rejected — this is the design being superseded |
| Call the agentic parser on every replay | Simpler (no cache) | Non-deterministic replay (violates ADR-005), unbounded recurring cost, unbounded vendor exposure | Rejected |

## Consequences

- Subsystem 02 §2.1's tiered extractor is rewritten; the layout-signature/quarantine flow (02 §5) **survives unchanged and matters more than before** — an agentic parser fails *differently* (it hallucinates plausible numbers rather than returning nothing), so **validation and total-reconciliation are now the primary defence, not a safety net.**
- **Numeric reconciliation is mandatory, not optional.** Every parsed table must reconcile row/column totals against the printed totals; a bulletin that fails reconciliation is quarantined whole (`reason=totals_mismatch`). An OCR/VLM digit error on an epidemiological case count is exactly the silent corruption that destroys institutional trust.
- `parser_id@parser_version` joins `transform_version` as a first-class provenance field on every derived record.
- A new connector-SDK service, `ctx.parse_cache`, owns read-through caching against bronze. Connector authors never call the parser API directly.
- **Open:** verify whether the subscription bills per page or per document before committing to a quota (see 02 OQ-2).
