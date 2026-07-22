from __future__ import annotations

from datetime import datetime, timezone
import json
import uuid
from typing import Any
from confluent_kafka import Producer
from chip_extractors.base import ExtractionInput, SourceRecord


class KafkaProducerClient:
    """CloudEvents 1.0 Kafka Producer for CHIP extractors."""

    def __init__(self, bootstrap_servers: str = "localhost:9092", producer: Any | None = None) -> None:
        self.bootstrap_servers = bootstrap_servers
        if producer is not None:
            self._producer = producer
        else:
            self._producer = Producer({"bootstrap.servers": self.bootstrap_servers})

    def produce_cloudevent(
        self,
        topic: str,
        record: SourceRecord,
        inp: ExtractionInput,
        extractor_name: str,
    ) -> None:
        """Wrap SourceRecord in a CloudEvents 1.0 envelope and produce to Kafka."""
        now_iso = (record.occurred_at or datetime.now(timezone.utc)).isoformat()
        cloudevent_id = str(uuid.uuid4())

        envelope = {
            "specversion": "1.0",
            "id": cloudevent_id,
            "source": f"/chip/extractors/{extractor_name}",
            "type": "pk.chip.health.disease_case_report.v1",
            "time": now_iso,
            "datacontenttype": "application/json",
            "data": {
                "payload": record.payload,
                "provenance": {
                    "source": inp.source,
                    "identity": inp.identity,
                    "bronze_uri": inp.bronze_uri,
                    "content_hash": inp.content_hash,
                    "original_filename": inp.original_filename,
                },
            },
        }

        key_bytes = record.record_key.encode("utf-8") if record.record_key else None
        value_bytes = json.dumps(envelope).encode("utf-8")

        self._producer.produce(
            topic=topic,
            key=key_bytes,
            value=value_bytes,
        )

    def flush(self, timeout: float = 5.0) -> int:
        """Flush outstanding Kafka producer queue."""
        return self._producer.flush(timeout)
