# How to Add a Data Collection Connector to CHIP

This guide explains how to implement and register a new thin data collection connector in Layer 2 of the Climate-Health Intelligence Platform (CHIP).

---

## Architecture Overview

A connector is a **thin archival gateway**. It follows a 4-step lifecycle:
```
discover → fetch → archive_raw → signal
```

It does **NOT** parse tables, validate records, or produce domain entities — structure extraction is owned downstream by Layer 3 (Document Extractors).

---

## 5-Step Process to Add a Connector

### 1. Create a Package in `connectors/<source_slug>`

Copy an existing connector directory (e.g. `connectors/ajk_idsrs`) or create a new workspace package:

```
connectors/my_source/
├── pyproject.toml
├── config.yaml
├── my_source/
│   ├── __init__.py
│   └── connector.py
└── tests/
    ├── __init__.py
    └── test_my_source_discover.py
```

### 2. Implement the 3 Abstract Methods of `Connector`

In `connectors/my_source/my_source/connector.py`:

```python
from typing import Iterator
from datetime import datetime, timezone
from pathlib import Path
from chip_connectors.base import Connector, DiscoveredItem, RawArtifact, RunContext

class MySourceConnector(Connector):
    name = "my_source"
    connector_version = "1.0.0"
    content_type = "text/markdown" # or application/pdf, application/json, etc.

    def derive_identity(self, source_uri: str, ctx: RunContext | None = None) -> str:
        # Compute stable, deterministic identity key (e.g., "my_source:2025:W01")
        ...

    def discover(self, ctx: RunContext) -> Iterator[DiscoveredItem]:
        # Enumerate candidates and yield DiscoveredItem objects
        ...

    def fetch(self, item: DiscoveredItem, ctx: RunContext) -> RawArtifact:
        # Retrieve raw bytes and return RawArtifact
        ...
```

### 3. Register in `ingestion.extractor_registry`

Add your source and its downstream extractor to `migrations/V001__layer2_ingestion_schema.sql` (or insert directly via SQL):

```sql
INSERT INTO ingestion.extractor_registry (source, extractor_name)
VALUES ('my_source', 'my_source_extractor');
```

### 4. Create a Dagster Asset

In `pipelines/ingestion/assets.py`:

```python
@asset(group_name="ingestion", kinds={"minio", "postgres"})
def raw_my_source(context, connector_context: ConnectorContextResource):
    conn = MySourceConnector()
    run_ctx = connector_context.build_context(source=conn.name)
    summary = run_connector(conn, run_ctx)
    ...
```

### 5. Add Unit Tests and Run Pytest

Create `test_my_source_discover.py` in `connectors/my_source/tests/` and run:

```bash
uv run pytest
```

---

## Key Principles & Design Rules

1. **Bronze is the System of Record**: Every archived document is written to `s3://chip-bronze/<source>/<identity>/<hash>/<filename>` with an immutable `.meta.json` sidecar.
2. **Identity Key Stability**: The identity string (`my_source:2025:W01`) must be derived purely from metadata (filenames/URLs) without database lookups.
3. **Format-Change Resilience**: Format layout shifts quarantine downstream extractors in Layer 3, not the upstream connector.
