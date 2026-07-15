# Workflow
- Present options with pros, cons, and recommendations when design decisions are involved. Confidence: 0.85
- Do not assume — ask questions when encountering ambiguities in requirements or design. Confidence: 0.85
- Build prototypes with scalability in mind — design for the current static/demo case but keep the path to production real-time ingestion clear. Confidence: 0.80

# Documentation
- Use a structured folder hierarchy for architecture documentation (e.g., zoomed_in_layer/layer{N}/). Confidence: 0.70

# Architecture
- Filename normalization and stable identity derivation should live inside the connector (post-fetch, pre-archive), not as a separate pre-processing step. The original messy filename is preserved in metadata. Confidence: 0.70
