from unittest.mock import MagicMock
from chip_connectors.dedup import DedupStore


def test_dedup_store_methods():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    store = DedupStore(db_url_or_conn=mock_conn)

    # 1. identity_seen - false case
    mock_cursor.fetchone.return_value = (False,)
    assert store.identity_seen("nih_idsr", "idsr:2025:W01", conn=mock_conn) is False

    # 2. identity_seen - true case
    mock_cursor.fetchone.return_value = (True,)
    assert store.identity_seen("nih_idsr", "idsr:2025:W01", conn=mock_conn) is True

    # 3. record_identity
    store.record_identity("nih_idsr", "idsr:2025:W01", "sha256:abc", conn=mock_conn)
    assert mock_cursor.execute.call_count >= 3
