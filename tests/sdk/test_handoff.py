from datetime import datetime, timezone
from unittest.mock import MagicMock
from chip_connectors.base import RawDocumentRow
from chip_connectors.handoff import HandoffStore


def test_handoff_store_signal():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (42,)  # returned doc_id

    store = HandoffStore(db_url_or_conn=mock_conn)

    row = RawDocumentRow(
        source="nih_idsr",
        identity="idsr:2025:W01",
        bronze_uri="s3://chip-bronze/...",
        content_hash="sha256:abc",
        content_type="text/markdown",
        original_filename="Week-01-2025.md",
        source_uri="Data_sources_1/NIH/MD/Week-01-2025.md",
        connector_version="1.0.0",
        retrieved_at=datetime.now(timezone.utc),
        file_size_bytes=100,
    )

    doc_id = store.signal(row, conn=mock_conn)
    assert doc_id == 42


def test_handoff_get_registered_extractors():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("nih_idsr_disease_tables",)]

    store = HandoffStore(db_url_or_conn=mock_conn)
    extractors = store.get_registered_extractors("nih_idsr", conn=mock_conn)
    assert extractors == ["nih_idsr_disease_tables"]
