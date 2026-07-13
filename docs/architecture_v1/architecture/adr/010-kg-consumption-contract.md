# ADR-010: Knowledge-Graph Consumption Contract — Reified Assertions, No Raw Semantic Edges

- **Status:** Accepted (2026-07-10 — reconciliation decision; aligns serving layer to the CHKG design)
- **Context:** Subsystem 04 §1.2 forbids direct semantic edges: any causal/statistical link (`TRIGGERS`, `ASSOCIATED_WITH`) must be a reified `:Assertion` node carrying `assertion_type`, epistemic `status` (`observed`/`statistical`/`hypothesized`/`refuted`), confidence, model version, and `SUPPORTED_BY → :Evidence` — so provenance and epistemic honesty are structural (ADR-005). But the serving doc (06 §2.4) modelled alert evidence as a **direct** `(:HazardEvent)-[:INCREASES_RISK_OF {confidence}]->(:Disease)` edge. The serving layer would query a graph shape the KG builder never produces, and would silently present a hypothesis as a fact.

## Decision

**The CHKG public read contract is assertion-shaped. Consumers never assume raw semantic edges.**

1. **Definitional edges are plain edges** and may be traversed directly: `IN_LOCATION`, `DURING`, `OF_DISEASE`, `OF_VARIABLE`, `OCCURRED_IN`, `PRECEDES`, `PARENT_OF`, `REPORTED_IN`, `DERIVED_FROM`, `SUPPORTED_BY`.
2. **Semantic/causal/statistical links are `:Assertion` nodes**, read via `SUBJECT`/`OBJECT`/`SUPPORTED_BY` with `status` + `confidence` + `derivation` + `model_version`. There is **no** `INCREASES_RISK_OF`/`TRIGGERS`/`ASSOCIATED_WITH` relationship type in the contract.
3. **Serving (`kg/repo.py`) exposes an assertion-shaped view.** The alert-evidence endpoint (06 §2.4) returns, per contributing link: `assertion_type`, `status`, `confidence`, `subject`/`object` nodes, and the `Evidence` cards (quote, `minio_uri`, `model_version`) — exactly the shape of the §4.1 explainability query in 04. The dashboard **must render `status`** (observed vs statistical vs hypothesized) and word hypothesized links as hypotheses.
4. **Every issued alert's evidence bundle** dereferences to assertions with ≥1 `SUPPORTED_BY` (except `derivation='domain_prior'`), matching the governance audit in 04 §4.4 and the "no evidence bundle → cannot leave CANDIDATE" rule in 05 §5.7.

## Alternatives rejected

- **Raw typed edges with a `confidence` property (06's original sketch):** simpler Cypher and one fewer hop, but a relationship in Neo4j **cannot itself carry relationships**, so it cannot attach multiple evidence spans, model version, or epistemic status — and it silently encodes causation, which is precisely what a policy-facing, auditable system must not do (breaks ADR-005).
- **Hybrid (keep reified assertions for provenance *and* project a denormalized `INCREASES_RISK_OF` edge for fast reads):** tempting for query speed, but it creates two representations of the same truth that drift; at this graph scale the extra hop is free, so the denormalization buys nothing worth the sync risk.

## Consequences

- Supersedes the KG edge shape in 06 §2.4's evidence example (the `INCREASES_RISK_OF` edge becomes an `ASSOCIATED_WITH`/`TRIGGERS` assertion with `status`). The API response gains a `status` field per evidence link.
- The serving `kg` module's Cypher is the templated §4 explainability trace from 04, not ad-hoc edge walks — one place owns the query shape.
- Dashboard S3 ("why this alert") and the RAG citation cards both surface epistemic status, satisfying the proposal's explainability/auditability commitment end-to-end.
