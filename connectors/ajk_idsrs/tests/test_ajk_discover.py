from pathlib import Path
from unittest.mock import MagicMock
from ajk_idsrs.connector import AjkIdsrsConnector


def test_ajk_derive_identity():
    conn = AjkIdsrsConnector()
    identity = conn.derive_identity("IDSR-WEEKLY-BULLETIN-AJK_EPI-Week-18_2026.md")
    assert identity == "ajk_idsrs:2026:W18"


def test_ajk_discover_real_files():
    conn = AjkIdsrsConnector(source_dir="Data_sources_1/AJK/out/MD")
    ctx = MagicMock()
    items = list(conn.discover(ctx))

    assert len(items) == 3
    identities = [item.identity for item in items]
    assert "ajk_idsrs:2026:W18" in identities
    assert "ajk_idsrs:2026:W19" in identities
    assert "ajk_idsrs:2026:W20" in identities


def test_ajk_fetch():
    conn = AjkIdsrsConnector(source_dir="Data_sources_1/AJK/out/MD")
    ctx = MagicMock()
    items = list(conn.discover(ctx))
    assert len(items) > 0

    artifact = conn.fetch(items[0], ctx)
    assert artifact.content_type == "text/markdown"
    assert len(artifact.content) > 0
    assert artifact.original_filename.endswith(".md")
