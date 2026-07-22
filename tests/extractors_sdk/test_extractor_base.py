from datetime import datetime, timezone
import pytest
from chip_extractors.base import ExtractionInput, Extractor, SourceRecord, RunSummary


def test_source_record_creation():
    rec = SourceRecord(
        payload={"district": "D.G KHAN", "disease": "ILI", "cases": 150},
        record_key="D.G KHAN|ILI|2025-W01",
        occurred_at=datetime.now(timezone.utc),
    )
    assert rec.payload["district"] == "D.G KHAN"
    assert rec.record_key == "D.G KHAN|ILI|2025-W01"


def test_extractor_abc_cannot_be_instantiated():
    class IncompleteExtractor(Extractor):
        pass

    with pytest.raises(TypeError):
        IncompleteExtractor()
