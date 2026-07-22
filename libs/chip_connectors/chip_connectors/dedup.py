from __future__ import annotations

import psycopg


class DedupStore:
    """Postgres-backed deduplication store for identity and content hashes."""

    def __init__(self, db_url_or_conn: str | psycopg.Connection) -> None:
        self.db_url_or_conn = db_url_or_conn

    def _get_connection(self) -> psycopg.Connection:
        if isinstance(self.db_url_or_conn, psycopg.Connection):
            return self.db_url_or_conn
        return psycopg.connect(self.db_url_or_conn)

    def identity_seen(self, source: str, identity: str, conn: psycopg.Connection | None = None) -> bool:
        """Check if an identity key has already been recorded for a source."""
        query = """
            SELECT EXISTS (
                SELECT 1 FROM ingestion.dedup_state
                WHERE source = %s AND identity = %s
            );
        """
        if conn:
            with conn.cursor() as cur:
                cur.execute(query, (source, identity))
                row = cur.fetchone()
                return row[0] if row else False
        else:
            with self._get_connection() as connection:
                with connection.cursor() as cur:
                    cur.execute(query, (source, identity))
                    row = cur.fetchone()
                    return row[0] if row else False

    def content_seen(self, source: str, content_hash: str, conn: psycopg.Connection | None = None) -> bool:
        """Check if a content hash has already been recorded for a source."""
        query = """
            SELECT EXISTS (
                SELECT 1 FROM ingestion.dedup_state
                WHERE source = %s AND content_hash = %s
            );
        """
        if conn:
            with conn.cursor() as cur:
                cur.execute(query, (source, content_hash))
                row = cur.fetchone()
                return row[0] if row else False
        else:
            with self._get_connection() as connection:
                with connection.cursor() as cur:
                    cur.execute(query, (source, content_hash))
                    row = cur.fetchone()
                    return row[0] if row else False

    def record_identity(
        self, source: str, identity: str, content_hash: str = "", conn: psycopg.Connection | None = None
    ) -> None:
        """Record an identity key in dedup_state."""
        query = """
            INSERT INTO ingestion.dedup_state (source, identity, content_hash, first_seen_at, last_seen_at)
            VALUES (%s, %s, %s, now(), now())
            ON CONFLICT (source, identity) DO UPDATE SET
                last_seen_at = now(),
                content_hash = CASE WHEN EXCLUDED.content_hash <> '' THEN EXCLUDED.content_hash ELSE ingestion.dedup_state.content_hash END;
        """
        if conn:
            with conn.cursor() as cur:
                cur.execute(query, (source, identity, content_hash))
        else:
            with self._get_connection() as connection:
                with connection.cursor() as cur:
                    cur.execute(query, (source, identity, content_hash))
                connection.commit()

    def record_content(self, source: str, content_hash: str, conn: psycopg.Connection | None = None) -> None:
        """Record a content hash in dedup_state (updates existing or inserts placeholder identity if needed)."""
        # Search if there is a row for this source with this content_hash
        query = """
            SELECT 1 FROM ingestion.dedup_state WHERE source = %s AND content_hash = %s LIMIT 1;
        """
        if conn:
            with conn.cursor() as cur:
                cur.execute(query, (source, content_hash))
                if cur.fetchone():
                    return
        else:
            with self._get_connection() as connection:
                with connection.cursor() as cur:
                    cur.execute(query, (source, content_hash))
                    if cur.fetchone():
                        return
