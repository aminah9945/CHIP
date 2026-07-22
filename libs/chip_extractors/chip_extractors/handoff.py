from __future__ import annotations

import psycopg
from chip_extractors.base import ExtractionInput


class HandoffClient:
    """Postgres handoff client for Layer 3 extractors.

    Polls pending items from raw_documents JOIN extractor_status,
    and updates extraction status (pending -> extracting -> extracted / failed).
    """

    def __init__(self, db_url_or_conn: str | psycopg.Connection) -> None:
        self.db_url_or_conn = db_url_or_conn

    def _get_connection(self) -> psycopg.Connection:
        if isinstance(self.db_url_or_conn, psycopg.Connection):
            return self.db_url_or_conn
        return psycopg.connect(self.db_url_or_conn)

    def poll_pending(
        self, extractor_name: str, limit: int = 100, conn: psycopg.Connection | None = None
    ) -> list[ExtractionInput]:
        """Query pending work items for an extractor from ingestion.extractor_status JOIN raw_documents."""
        query = """
            SELECT
                rd.id, rd.source, rd.identity, rd.bronze_uri, rd.content_hash,
                rd.content_type, rd.original_filename, rd.source_uri,
                rd.connector_version, rd.retrieved_at, rd.file_size_bytes
            FROM ingestion.raw_documents rd
            JOIN ingestion.extractor_status es ON es.raw_document_id = rd.id
            WHERE es.extractor_name = %s AND es.status = 'pending'
            ORDER BY rd.created_at ASC
            LIMIT %s;
        """
        inputs: list[ExtractionInput] = []

        def parse_rows(rows):
            for r in rows:
                inputs.append(
                    ExtractionInput(
                        id=r[0],
                        source=r[1],
                        identity=r[2],
                        bronze_uri=r[3],
                        content_hash=r[4],
                        content_type=r[5],
                        original_filename=r[6],
                        source_uri=r[7],
                        connector_version=r[8],
                        retrieved_at=r[9],
                        file_size_bytes=r[10],
                    )
                )

        if conn:
            with conn.cursor() as cur:
                cur.execute(query, (extractor_name, limit))
                parse_rows(cur.fetchall())
        else:
            with self._get_connection() as connection:
                with connection.cursor() as cur:
                    cur.execute(query, (extractor_name, limit))
                    parse_rows(cur.fetchall())

        return inputs

    def update_status(
        self,
        raw_document_id: int,
        extractor_name: str,
        status: str,
        records_produced: int = 0,
        error_message: str | None = None,
        conn: psycopg.Connection | None = None,
    ) -> None:
        """Update the status of an extraction task in ingestion.extractor_status."""
        query = """
            UPDATE ingestion.extractor_status
            SET
                status = %s,
                records_produced = CASE WHEN %s = 'extracted' THEN %s ELSE records_produced END,
                error_message = %s,
                error_at = CASE WHEN %s = 'failed' THEN now() ELSE error_at END,
                started_at = CASE WHEN %s = 'extracting' THEN now() ELSE started_at END,
                completed_at = CASE WHEN %s IN ('extracted', 'failed') THEN now() ELSE completed_at END
            WHERE raw_document_id = %s AND extractor_name = %s;
        """
        params = (
            status,
            status,
            records_produced,
            error_message,
            status,
            status,
            status,
            raw_document_id,
            extractor_name,
        )

        if conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
        else:
            with self._get_connection() as connection:
                with connection.cursor() as cur:
                    cur.execute(query, params)
                connection.commit()
