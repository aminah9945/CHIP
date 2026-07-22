from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
from pitb_dss_disease_tables.extractor import PitbDssDiseaseTableExtractor
from chip_extractors.base import ExtractionInput


def test_pitb_extractor_real_file():
    extractor = PitbDssDiseaseTableExtractor()
    sample_path = Path("Data_sources_1/PITB-DSS/2015/MD/DSS-Bulletin-Week-11.md")
    content = sample_path.read_bytes()

    inp = ExtractionInput(
        id=1,
        source="pitb_dss",
        identity="pitb_dss:2015:W11",
        bronze_uri="s3://chip-bronze/pitb_dss/...",
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
    disease_names = [r.payload["disease_raw"] for r in records]
    assert "Diarrhoea (Acute)" in disease_names or "Acute (upper) Respiratory Infections" in disease_names
