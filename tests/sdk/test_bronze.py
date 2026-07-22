from unittest.mock import MagicMock
from chip_connectors.bronze import BronzeClient


def test_bronze_client_archive():
    mock_minio = MagicMock()
    mock_minio.bucket_exists.return_value = True

    client = BronzeClient(
        endpoint="localhost:9000",
        access_key="key",
        secret_key="secret",
        bucket="chip-bronze",
        client=mock_minio,
    )

    uri = client.archive(
        source="nih_idsr",
        identity="idsr:2025:W01",
        content=b"# Header\nTest content",
        content_type="text/markdown",
        content_hash="sha256:abc123def456",
        original_filename="Week-01-2025.md",
        metadata={"source_uri": "Data_sources_1/NIH/MD/Week-01-2025.md"},
    )

    assert uri == "s3://chip-bronze/nih_idsr/idsr:2025:W01/sha256-abc123def456/Week-01-2025.md"
    assert mock_minio.put_object.call_count == 2  # raw content + .meta.json sidecar


def test_bronze_client_get():
    mock_minio = MagicMock()
    mock_response = MagicMock()
    mock_response.read.return_value = b"# Content"
    mock_minio.get_object.return_value = mock_response

    client = BronzeClient(
        endpoint="localhost:9000",
        access_key="key",
        secret_key="secret",
        bucket="chip-bronze",
        client=mock_minio,
    )

    content = client.get("s3://chip-bronze/nih_idsr/idsr:2025:W01/sha256-abc123/Week-01-2025.md")
    assert content == b"# Content"
    mock_minio.get_object.assert_called_once_with("chip-bronze", "nih_idsr/idsr:2025:W01/sha256-abc123/Week-01-2025.md")
