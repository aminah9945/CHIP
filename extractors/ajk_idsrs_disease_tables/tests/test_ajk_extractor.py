from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
from ajk_idsrs_disease_tables.extractor import AjkIdsrsDiseaseTableExtractor
from chip_extractors.base import ExtractionInput


def test_ajk_extractor_real_file():
    extractor = AjkIdsrsDiseaseTableExtractor()
    sample_path = Path("Data_sources_1/AJK/out/MD/IDSR-WEEKLY-BULLETIN-AJK_EPI-Week-18_2026.md")
    content = sample_path.read_bytes()

    inp = ExtractionInput(
        id=1,
        source="ajk_idsrs",
        identity="ajk_idsrs:2026:W18",
        bronze_uri="s3://chip-bronze/ajk_idsrs/...",
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
    # Check overall cases payload
    overall_recs = [r for r in records if r.payload.get("scope") == "overall_ajk"]
    assert len(overall_recs) > 0
    assert overall_recs[0].payload["district"] == "AJ&K"

    # Check district detail payload
    district_recs = [r for r in records if r.payload.get("scope") == "district_detail"]
    assert len(district_recs) > 0
