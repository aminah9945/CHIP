from datetime import datetime, timezone
import pytest
from chip_connectors.base import Connector, DiscoveredItem, RawArtifact, RawDocumentRow, RunSummary


def test_discovered_item_immutable():
    item = DiscoveredItem(source_uri="file.md", identity="id:1", hints={"key": "val"})
    assert item.source_uri == "file.md"
    assert item.identity == "id:1"
    with pytest.raises(AttributeError):
        item.identity = "id:2"  # frozen


def test_raw_artifact_hash():
    item = DiscoveredItem(source_uri="file.md", identity="id:1")
    content = b"hello world"
    artifact = RawArtifact(
        item=item,
        content=content,
        content_type="text/markdown",
        fetched_at=datetime.now(timezone.utc),
        original_filename="file.md",
    )
    # sha256 of "hello world" is b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
    assert artifact.content_hash == "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"


def test_connector_abc():
    class IncompleteConnector(Connector):
        pass

    with pytest.raises(TypeError):
        IncompleteConnector()
