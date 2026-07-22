from datetime import datetime, timezone
from typing import Iterator
from unittest.mock import MagicMock
from chip_connectors.base import Connector, DiscoveredItem, RawArtifact, RunContext
from chip_connectors.runner import run_connector


class DummyConnector(Connector):
    name = "dummy_source"
    connector_version = "1.0.0"
    content_type = "text/markdown"

    def discover(self, ctx: RunContext) -> Iterator[DiscoveredItem]:
        yield DiscoveredItem(source_uri="file1.md", identity="dummy:2025:W01")
        yield DiscoveredItem(source_uri="file2.md", identity="dummy:2025:W02")

    def derive_identity(self, source_uri: str, ctx: RunContext) -> str:
        return "dummy:identity"

    def fetch(self, item: DiscoveredItem, ctx: RunContext) -> RawArtifact:
        return RawArtifact(
            item=item,
            content=f"Content for {item.identity}".encode("utf-8"),
            content_type="text/markdown",
            fetched_at=datetime.now(timezone.utc),
            original_filename="file.md",
        )


def test_runner_happy_path():
    ctx = MagicMock()
    ctx.dedup.identity_seen.return_value = False
    ctx.dedup.content_seen.return_value = False
    ctx.bronze.archive.return_value = "s3://chip-bronze/..."
    ctx.handoff.signal.return_value = 100
    ctx.handoff.get_registered_extractors.return_value = ["dummy_extractor"]

    conn = DummyConnector()
    summary = run_connector(conn, ctx)

    assert summary.discovered == 2
    assert summary.fetched == 2
    assert summary.archived == 2
    assert summary.skipped_identity == 0
    assert summary.skipped_content == 0
    assert summary.errors == 0


def test_runner_identity_dedup_skip():
    ctx = MagicMock()
    # First item seen, second unseen
    ctx.dedup.identity_seen.side_effect = [True, False]
    ctx.dedup.content_seen.return_value = False
    ctx.bronze.archive.return_value = "s3://chip-bronze/..."
    ctx.handoff.signal.return_value = 101
    ctx.handoff.get_registered_extractors.return_value = []

    conn = DummyConnector()
    summary = run_connector(conn, ctx)

    assert summary.discovered == 2
    assert summary.fetched == 1
    assert summary.archived == 1
    assert summary.skipped_identity == 1
