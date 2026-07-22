from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
from typing import Any, Iterator


@dataclass(frozen=True)
class DiscoveredItem:
    """One candidate document found during discover()."""

    source_uri: str
    identity: str
    hints: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RawArtifact:
    """Bytes fetched from the source, before any processing."""

    item: DiscoveredItem
    content: bytes
    content_type: str
    fetched_at: datetime
    original_filename: str
    http_meta: dict[str, Any] = field(default_factory=dict)

    @property
    def content_hash(self) -> str:
        """SHA-256 hash formatted as sha256:<hex>."""
        return "sha256:" + hashlib.sha256(self.content).hexdigest()


@dataclass(frozen=True)
class RawDocumentRow:
    """The row written to ingestion.raw_documents after archival.

    Does NOT carry a status field — extraction state is tracked in
    the ingestion.extractor_status table (Layer 3).
    """

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


@dataclass
class RunSummary:
    """Summary metrics for one connector run."""

    source: str
    discovered: int = 0
    fetched: int = 0
    archived: int = 0
    skipped_identity: int = 0
    skipped_content: int = 0
    errors: int = 0
    duration_ms: int = 0


class Connector(ABC):
    """Thin archival gateway interface.

    Discovers, fetches, and archives raw documents.
    Does NOT parse, validate, or produce structured records.
    """

    name: str
    connector_version: str
    content_type: str  # e.g., "text/markdown", "application/pdf"

    @abstractmethod
    def discover(self, ctx: RunContext) -> Iterator[DiscoveredItem]:
        """Enumerate candidate documents and compute stable identity keys."""
        ...

    @abstractmethod
    def derive_identity(self, source_uri: str, ctx: RunContext) -> str:
        """Compute the stable natural key from a source URI/filename."""
        ...

    @abstractmethod
    def fetch(self, item: DiscoveredItem, ctx: RunContext) -> RawArtifact:
        """Retrieve raw bytes for one discovered item."""
        ...


@dataclass
class RunContext:
    """Container for injected infrastructure services required during a connector run."""

    bronze: Any  # BronzeClient
    dedup: Any  # DedupStore
    handoff: Any  # HandoffStore
    log: Any  # structlog BoundLogger
    metrics: Any  # MetricsEmitter
