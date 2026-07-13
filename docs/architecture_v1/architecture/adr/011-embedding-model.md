# ADR-011: Multilingual Embedding Model — BGE-M3 (1024-dim)

- **Status:** Accepted (2026-07-10 — reconciliation decision; pins the pgvector dimension). Confirm Urdu retrieval quality with the NLP team before the at-scale HNSW build (was 01 OQ-7).
- **Context:** The pgvector column was declared `VECTOR(1024)` in subsystem 01 §5.7 (assuming a bge-m3-class model), while the NLP doc (03) named `multilingual-e5-base`/LaBSE (typically **768-dim**) and the CHKG/RAG doc (04 §6.2) said only "a strong open multilingual model." An embedding dimension mismatch is not a soft disagreement: it is a table rebuild and a re-embed of the whole corpus if discovered late, and old/new vectors cannot share one HNSW index.

## Decision

1. **Platform embedding model: `BAAI/bge-m3`, dense output, 1024 dimensions**, self-hosted on the GPU box via the same inference service (03 §6.3), for **both** the NLP pipeline's document/signal embeddings **and** the graph-RAG chunk embeddings (04 §6). One model, one dimension, one `rag_chunk.embedding VECTOR(1024)` (01 §5.7 unchanged).
2. **`embed_model` + `embed_version` are stored on every chunk** (01 §5.7); a model change is a **versioned re-embed batch** so old and new vectors coexist during migration (never an in-place dimension change).
3. **Validation before scale:** confirm bge-m3 Urdu retrieval quality on the gold geo/RAG set (03 §7.3, 04 §6.6) before building the HNSW index over the full corpus.

## Alternatives rejected

- **`multilingual-e5-base` (768-dim):** lighter and fast, and fine for English, but weaker on Urdu and shorter max context; the whole novelty of CHIP is Urdu quality, so we do not economize here. (e5 remains a valid *benchmark comparator*, not the production embedder.)
- **LaBSE (768-dim):** strong sentence-level cross-lingual alignment across 109 languages, but sentence-oriented and older; bge-m3 gives longer context (up to 8k), stronger retrieval benchmarks, and native multi-granularity (dense/sparse/colbert) we can grow into.
- **A larger 1024+ model (e.g. bge-m3 is already the sweet spot):** bigger multilingual embedders exist but cost more VRAM that is already contended by the LLM and the encoder NER/RE services (03 §6.3) for marginal gains at our volume.

## Consequences

- 01's `VECTOR(1024)` and HNSW index stand; 03's embedding role and 04's RAG retrieval both bind to bge-m3.
- The embedding model is versioned in MLflow (03 §7.1) like any other model; re-embeds are additive batches.
- 01 OQ-7 is closed pending the Urdu quality spike.
