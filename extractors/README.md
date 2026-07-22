# How to Add a Document Extractor to CHIP

This guide explains how to implement and register a new format-aware **Document Extractor** in Layer 3 of the Climate-Health Intelligence Platform (CHIP).

---

## Architecture Overview

Extractors consume raw bronze document receipts from `ingestion.raw_documents`, parse embedded HTML/Markdown tables, fix table anomalies (e.g. multi-page page breaks), and publish **source-native records** in **CloudEvents 1.0 JSON format** to Kafka topics.

Extracted records retain exact string values as they appear in the source document without ontology mapping or P-code resolution (which is owned downstream by Layer 4 Normalizers).

```
ingestion.extractor_status (pending)
            │
            ▼
┌───────────────────────────────┐
│   run_extractor() Loop        │
│ 1. Set status = 'extracting'  │
│ 2. Fetch bytes from MinIO     │
│ 3. Parse & stitch tables      │
│ 4. Emit CloudEvents to Kafka  │
│ 5. Set status = 'extracted'   │
└───────────────────────────────┘
            │
            ▼
Kafka Topic (CloudEvents 1.0 JSON)
```

---

## 5-Step Process to Add an Extractor

### 1. Create a Package in `extractors/<extractor_name>`

Copy an existing extractor directory (e.g. `extractors/ajk_idsrs_disease_tables`) or create a new workspace package:

```
extractors/my_source_tables/
├── pyproject.toml
├── config.yaml
├── my_source_tables/
│   ├── __init__.py
│   └── extractor.py
└── tests/
    ├── __init__.py
    └── test_my_extractor.py
```

### 2. Implement the `extract()` Method

In `extractors/my_source_tables/my_source_tables/extractor.py`:

```python
from typing import Iterator
from chip_extractors.base import ExtractionInput, Extractor, ExtractorContext, SourceRecord
from chip_extractors.table_parser import TableParser

class MySourceTableExtractor(Extractor):
    name = "my_source_tables"
    extractor_version = "1.0.0"
    kafka_topic = "chip.health.my_source.disease_case_report.v1"
    input_content_type = "text/markdown"
    input_sources = ["my_source"]

    def __init__(self) -> None:
        self.parser = TableParser()

    def extract(self, inp: ExtractionInput, content: bytes, ctx: ExtractorContext) -> Iterator[SourceRecord]:
        text = content.decode("utf-8", errors="ignore")
        tables = self.parser.parse_tables(text)

        for table in tables:
            # Extract rows and yield SourceRecord objects
            ...
            yield SourceRecord(
                payload={"district": district, "disease_raw": disease, "cases_raw": cases},
                record_key=f"{district}|{disease}|{year_week}"
            )
```

### 3. Register in `ingestion.extractor_registry`

Register your source and extractor name in Postgres:

```sql
INSERT INTO ingestion.extractor_registry (source, extractor_name)
VALUES ('my_source', 'my_source_tables');
```

### 4. Create a Dagster Asset

In `pipelines/extraction/assets.py`:

```python
@asset(group_name="extraction", kinds={"kafka", "postgres", "minio"}, deps=[raw_my_source])
def extracted_my_source(context, extractor_context: ExtractorContextResource):
    extractor = MySourceTableExtractor()
    run_ctx = extractor_context.build_context(extractor_name=extractor.name)
    summary = run_extractor(extractor, run_ctx)
    ...
```

### 5. Run Unit Tests

```bash
uv run pytest extractors/my_source_tables/tests/
```

---

## Design Principles

1. **Source-Native Granularity**: Do not alter disease names or resolve location aliases in Layer 3.
2. **CloudEvents 1.0 Auditability**: All records emitted to Kafka contain full provenance (`source`, `identity`, `bronze_uri`, `content_hash`).
3. **Table Stitching**: Multi-page tables split by Markdown page breaks are automatically stitched by `TableParser`.
