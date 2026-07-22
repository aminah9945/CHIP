import concurrent.futures
from unittest.mock import MagicMock
from ajk_idsrs.connector import AjkIdsrsConnector
from chip_connectors.runner import run_connector


def test_concurrent_connector_runs():
    conn = AjkIdsrsConnector(source_dir="Data_sources_1/AJK/out/MD")

    def run_one():
        ctx = MagicMock()
        ctx.dedup.identity_seen.return_value = False
        ctx.dedup.content_seen.return_value = False
        ctx.bronze.archive.return_value = "s3://chip-bronze/..."
        ctx.handoff.signal.return_value = 1
        ctx.handoff.get_registered_extractors.return_value = []
        return run_connector(conn, ctx)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(run_one) for _ in range(4)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert len(results) == 4
    for summary in results:
        assert summary.discovered == 3
        assert summary.errors == 0
