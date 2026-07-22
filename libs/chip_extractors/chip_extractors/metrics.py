from __future__ import annotations

from datetime import datetime, timezone
import psycopg
from chip_extractors.base import RunSummary


class MetricsEmitter:
    """Emits RunSummary metrics for extractors."""

    def __init__(self, db_url_or_conn: str | psycopg.Connection) -> None:
        self.db_url_or_conn = db_url_or_conn

    def emit(
        self,
        summary: RunSummary,
        extractor_version: str = "1.0.0",
        started_at: datetime | None = None,
        conn: psycopg.Connection | None = None,
    ) -> None:
        """Log or emit extractor run summary metrics."""
        # For now, summary metrics are handled via structlog and recorded on extractor_status
        pass
