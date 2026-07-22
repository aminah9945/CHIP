from __future__ import annotations

from datetime import datetime, timezone
import psycopg
from chip_connectors.base import RunSummary


class MetricsEmitter:
    """Emits RunSummary metrics to ingestion.connector_runs table."""

    def __init__(self, db_url_or_conn: str | psycopg.Connection) -> None:
        self.db_url_or_conn = db_url_or_conn

    def _get_connection(self) -> psycopg.Connection:
        if isinstance(self.db_url_or_conn, psycopg.Connection):
            return self.db_url_or_conn
        return psycopg.connect(self.db_url_or_conn)

    def emit(
        self,
        summary: RunSummary,
        connector_version: str = "1.0.0",
        started_at: datetime | None = None,
        conn: psycopg.Connection | None = None,
    ) -> None:
        """Write a RunSummary entry to ingestion.connector_runs."""
        if started_at is None:
            started_at = datetime.now(timezone.utc)

        query = """
            INSERT INTO ingestion.connector_runs (
                source, connector_version, discovered, fetched, archived,
                skipped_identity, skipped_content, errors, duration_ms, started_at, finished_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
            );
        """
        params = (
            summary.source,
            connector_version,
            summary.discovered,
            summary.fetched,
            summary.archived,
            summary.skipped_identity,
            summary.skipped_content,
            summary.errors,
            max(0, int(summary.duration_ms)),
            started_at,
        )

        if conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
        else:
            with self._get_connection() as connection:
                with connection.cursor() as cur:
                    cur.execute(query, params)
                connection.commit()
