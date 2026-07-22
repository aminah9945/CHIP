from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterator


@dataclass(frozen=True)
class ExtractionInput:
    """One row from ingestion.raw_documents passed to the extractor."""

    id: int
    source: str
    identity: str
    bronze_uri: str
    content_hash: str
    content_type: str
    original_filename: str
    source_uri: str
    connector_version: str
    retrieved_at: datetime
    file_size_bytes: int


@dataclass(frozen=True)
class SourceRecord:
    """One extracted record at source-native granularity.

    All field values match exactly as they appear in the source document.
    No canonical mapping, no disease-code translation, no P-code resolution.
    That is Layer 4's job.
    """

    payload: dict[str, Any]
    record_key: str  # Kafka message key: "<district>|<disease>|<year>-W<week>"
    occurred_at: datetime | None = None


@dataclass
class RunSummary:
    """Summary metrics for one extractor run."""

    extractor: str
    documents_read: int = 0
    documents_extracted: int = 0
    records_produced: int = 0
    errors: int = 0
    duration_ms: int = 0


@dataclass
class ExtractorContext:
    """Container for injected infrastructure services required during an extractor run."""

    bronze: Any  # BronzeClient (from chip_connectors)
    handoff: Any  # HandoffClient
    kafka: Any  # KafkaProducerClient
    table_parser: Any  # TableParser
    log: Any  # structlog BoundLogger
    metrics: Any  # MetricsEmitter


class Extractor(ABC):
    """Format-aware adapter interface for extracting structured records from raw bronze artifacts."""

    name: str
    extractor_version: str
    kafka_topic: str
    input_content_type: str  # e.g. "text/markdown"
    input_sources: list[str]  # source slugs handled by this extractor

    @abstractmethod
    def extract(
        self,
        inp: ExtractionInput,
        content: bytes,
        ctx: ExtractorContext,
    ) -> Iterator[SourceRecord]:
        """Parse raw bronze content and yield source-native records."""
        ...
