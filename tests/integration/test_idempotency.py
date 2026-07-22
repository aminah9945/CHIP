from unittest.mock import MagicMock
from ajk_idsrs.connector import AjkIdsrsConnector
from chip_connectors.runner import run_connector


def test_idempotent_rerun():
    conn = AjkIdsrsConnector(source_dir="Data_sources_1/AJK/out/MD")

    # Run 1: First run - nothing seen
    ctx1 = MagicMock()
    ctx1.dedup.identity_seen.return_value = False
    ctx1.dedup.content_seen.return_value = False
    ctx1.bronze.archive.return_value = "s3://chip-bronze/..."
    ctx1.handoff.signal.return_value = 1
    ctx1.handoff.get_registered_extractors.return_value = []

    summary1 = run_connector(conn, ctx1)
    assert summary1.discovered == 3
    assert summary1.archived == 3
    assert summary1.skipped_identity == 0

    # Run 2: Re-run - all identities seen
    ctx2 = MagicMock()
    ctx2.dedup.identity_seen.return_value = True

    summary2 = run_connector(conn, ctx2)
    assert summary2.discovered == 3
    assert summary2.archived == 0
    assert summary2.skipped_identity == 3
