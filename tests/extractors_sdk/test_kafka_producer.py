from datetime import datetime, timezone
import json
from unittest.mock import MagicMock
from chip_extractors.base import ExtractionInput, SourceRecord
from chip_extractors.kafka_producer import KafkaProducerClient


def test_produce_cloudevent_envelope():
    mock_producer = MagicMock()
    client = KafkaProducerClient(bootstrap_servers="localhost:9094", producer=mock_producer)

    inp = ExtractionInput(
        id=1,
        source="nih_idsr",
        identity="idsr:2025:W01",
        bronze_uri="s3://chip-bronze/...",
        content_hash="sha256:abc",
        content_type="text/markdown",
        original_filename="Week-01-2025.md",
        source_uri="Data_sources_1/NIH/MD/Week-01-2025.md",
        connector_version="1.0.0",
        retrieved_at=datetime.now(timezone.utc),
        file_size_bytes=1000,
    )

    rec = SourceRecord(
        payload={"district": "D.G KHAN", "cases": 50},
        record_key="D.G KHAN|ILI|2025-W01",
    )

    client.produce_cloudevent("chip.health.nih_idsr.disease_case_report.v1", rec, inp, "nih_idsr_disease_tables")

    mock_producer.produce.assert_called_once()
    call_args = mock_producer.produce.call_args.kwargs

    assert call_args["topic"] == "chip.health.nih_idsr.disease_case_report.v1"
    assert call_args["key"] == b"D.G KHAN|ILI|2025-W01"

    envelope = json.loads(call_args["value"].decode("utf-8"))
    assert envelope["specversion"] == "1.0"
    assert envelope["source"] == "/chip/extractors/nih_idsr_disease_tables"
    assert envelope["data"]["payload"]["district"] == "D.G KHAN"
    assert envelope["data"]["provenance"]["identity"] == "idsr:2025:W01"
