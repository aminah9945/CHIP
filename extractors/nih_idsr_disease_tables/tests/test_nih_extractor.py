from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
from nih_idsr_disease_tables.extractor import NihIdsrDiseaseTableExtractor
from chip_extractors.base import ExtractionInput


def test_nih_extractor_era1_file():
    extractor = NihIdsrDiseaseTableExtractor()
    sample_path = Path("Data_sources_1/NIH/MD/IDSR-Weekly-Report-11-2022.md")
    content = sample_path.read_bytes()

    inp = ExtractionInput(
        id=1,
        source="nih_idsr",
        identity="idsr:2022:W11",
        bronze_uri="s3://chip-bronze/nih_idsr/...",
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


def test_nih_extractor_era2_file():
    extractor = NihIdsrDiseaseTableExtractor()
    sample_path = Path("Data_sources_1/NIH/MD/Week-01-2025.md")
    content = sample_path.read_bytes()

    inp = ExtractionInput(
        id=2,
        source="nih_idsr",
        identity="idsr:2025:W01",
        bronze_uri="s3://chip-bronze/nih_idsr/...",
        content_hash="sha256:def",
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
    provinces = {r.payload.get("province") for r in records if "province" in r.payload}
    assert "AJK" in provinces or "Balochistan" in provinces
