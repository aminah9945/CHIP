# Workflow
- Present options with pros, cons, and recommendations when design decisions are involved. Confidence: 0.85
- Do not assume — ask questions when encountering ambiguities in requirements or design. Confidence: 0.85
- Build prototypes with scalability in mind — design for the current static/demo case but keep the path to production real-time ingestion clear. Confidence: 0.80
- Resolve non-blocking design questions decisively — either defer to the appropriate downstream layer with rationale or close them. Don't let open questions accumulate when they don't block the current phase. Confidence: 0.70

# Documentation
- Use a structured folder hierarchy for architecture documentation (e.g., zoomed_in_layer/layer{N}/). Confidence: 0.70

# Architecture
- Filename normalization and stable identity derivation should live inside the connector (post-fetch, pre-archive), not as a separate pre-processing step. The original messy filename is preserved in metadata. Confidence: 0.70
- Design connector abstractions to be loosely coupled to data sources — adding or removing a source should require minimal effort. The connector SDK lifecycle contract should be source-agnostic. Confidence: 0.85
- Connectors should NOT do table parsing or content extraction — they should only discover, fetch, and archive raw data to bronze. Parsing and extraction happen downstream in Layer 3. This keeps raw data untouched for replay and allows different consumers to extract different things (e.g., prose vs tables) from the same archived source. Confidence: 0.85
- Extractors should be thin and structural-only — find tables, extract rows, type-check. Semantic mapping (disease labels, district names, epiweeks, reconciliation) belongs in the normalizer, not the extractor. The extractor understands document structure; the normalizer understands data semantics. Confidence: 0.80
- Optimize prototypes for the current data format rather than building dual-mode abstractions (e.g., local + HTTP) from day one. Build the simple thing that works now; refactor when the next format arrives. Confidence: 0.75
