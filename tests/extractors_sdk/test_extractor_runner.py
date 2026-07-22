from datetime import datetime, timezone
from typing import Iterator
from unittest.mock import MagicMock
from chip_extractors.base import ExtractionInput, Extractor, ExtractorContext, SourceRecord
from chip_extractors.runner import run_extractor


class DummyExtractor(Extractor):
    name = "dummy_extractor"
    extractor_version = "1.0.0"
    kafka_topic = "chip.health.dummy.v1"
    input_content_type = "text/markdown"
    input_sources = ["dummy_source"]

    def extract(self, inp: ExtractionInput, content: bytes, ctx: ExtractorContext) -> Iterator[SourceRecord]:
        yield SourceRecord(payload={"disease": "ILI", "cases": 10}, record_key="D1|ILI|2025-W01")
        yield SourceRecord(payload={"disease": "Malaria", "cases": 5}, record_key="D1|Malaria|2025-W01")


def test_extractor_runner_happy_path():
    ctx = MagicMock()

    dummy_input = ExtractionInput(
        id=1,
        source="dummy_source",
        identity="dummy:2025:W01",
        bronze_uri="s3://chip-bronze/...",
        content_hash="sha256:abc",
        content_type="text/markdown",
        original_filename="dummy.md",
        source_uri="Data_sources_1/dummy.md",
        connector_version="1.0.0",
        retrieved_at=datetime.now(timezone.utc),
        file_size_bytes=100,
    )

    ctx.handoff.poll_pending.return_value = [dummy_input]
    ctx.bronze.get.return_value = b"# Content"

    extractor = DummyExtractor()
    summary = run_extractor(extractor, ctx)

    assert summary.documents_read == 1
    assert summary.documents_extracted == 1
    assert summary.records_produced == 2
    assert summary.errors == 0
    assert ctx.kafka.produce_cloudevent.call_count == 2
    assert ctx.handoff.update_status.call_count == 2
