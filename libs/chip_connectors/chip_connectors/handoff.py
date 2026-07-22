from __future__ import annotations

import psycopg
from chip_connectors.base import RawDocumentRow


class HandoffStore:
    """Handoff store for signalling downstream raw_documents and seeding extractor_status."""

    def __init__(self, db_url_or_conn: str | psycopg.Connection) -> None:
        self.db_url_or_conn = db_url_or_conn

    def _get_connection(self) -> psycopg.Connection:
        if isinstance(self.db_url_or_conn, psycopg.Connection):
            return self.db_url_or_conn
        return psycopg.connect(self.db_url_or_conn)

    def signal(self, row: RawDocumentRow, conn: psycopg.Connection | None = None) -> int:
        """Insert a row into ingestion.raw_documents and return the generated ID."""
        query = """
            INSERT INTO ingestion.raw_documents (
                source, identity, bronze_uri, content_hash, content_type,
                original_filename, source_uri, connector_version, retrieved_at, file_size_bytes
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) RETURNING id;
        """
        params = (
            row.source,
            row.identity,
            row.bronze_uri,
            row.content_hash,
            row.content_type,
            row.original_filename,
            row.source_uri,
            row.connector_version,
            row.retrieved_at,
            row.file_size_bytes,
        )

        if conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                res = cur.fetchone()
                if not res:
                    raise RuntimeError("Failed to insert raw_documents row")
                return res[0]
        else:
            with self._get_connection() as connection:
                with connection.cursor() as cur:
                    cur.execute(query, params)
                    res = cur.fetchone()
                    if not res:
                        raise RuntimeError("Failed to insert raw_documents row")
                    doc_id = res[0]
                connection.commit()
                return doc_id

    def get_registered_extractors(self, source: str, conn: psycopg.Connection | None = None) -> list[str]:
        """Query registered extractors for a source from ingestion.extractor_registry."""
        query = """
            SELECT extractor_name
            FROM ingestion.extractor_registry
            WHERE source = %s;
        """
        if conn:
            with conn.cursor() as cur:
                cur.execute(query, (source,))
                return [r[0] for r in cur.fetchall()]
        else:
            with self._get_connection() as connection:
                with connection.cursor() as cur:
                    cur.execute(query, (source,))
                    return [r[0] for r in cur.fetchall()]

    def seed_extractor_status(
        self, raw_document_id: int, extractor_name: str, conn: psycopg.Connection | None = None
    ) -> None:
        """Create a pending status row in ingestion.extractor_status for an extractor."""
        query = """
            INSERT INTO ingestion.extractor_status (raw_document_id, extractor_name, status)
            VALUES (%s, %s, 'pending')
            ON CONFLICT (raw_document_id, extractor_name) DO NOTHING;
        """
        if conn:
            with conn.cursor() as cur:
                cur.execute(query, (raw_document_id, extractor_name))
        else:
            with self._get_connection() as connection:
                with connection.cursor() as cur:
                    cur.execute(query, (raw_document_id, extractor_name))
                connection.commit()
