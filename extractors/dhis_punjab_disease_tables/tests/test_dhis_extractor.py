from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
from dhis_punjab_disease_tables.extractor import DhisPunjabDiseaseTableExtractor
from chip_extractors.base import ExtractionInput


def test_dhis_extractor_real_file():
    extractor = DhisPunjabDiseaseTableExtractor()
    sample_path = Path("Data_sources_1/DHIS/MD/week_18.md")
    content = sample_path.read_bytes()

    inp = ExtractionInput(
        id=1,
        source="dhis_punjab_weekly",
        identity="dhis_punjab_weekly:2022:W18",
        bronze_uri="s3://chip-bronze/dhis_punjab_weekly/...",
        content_hash="sha256:abc",
        content_type="text/markdown",
        original_filename=sample_path.name,
        source_uri=str(sample_path),
        connector_version="1.0.0",
        retrieved_at=datetime.now(timezone.utc),
        file_size_bytes=len(content),
    )

    ctx = MagicMock()
    records = list(extractor.extract(inp, content, ctx))

    assert len(records) > 0
    districts = {r.payload["district"] for r in records}
    assert "Attock" in districts or "ATTOCK" in districts
