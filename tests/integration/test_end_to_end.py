from unittest.mock import MagicMock
from ajk_idsrs.connector import AjkIdsrsConnector
from dhis_punjab_weekly.connector import DhisPunjabWeeklyConnector
from nih_idsr.connector import NihIdsrConnector
from pitb_dss.connector import PitbDssConnector
from chip_connectors.runner import run_connector


def test_e2e_ajk_pipeline():
    conn = AjkIdsrsConnector(source_dir="Data_sources_1/AJK/out/MD")
    ctx = MagicMock()
    ctx.dedup.identity_seen.return_value = False
    ctx.dedup.content_seen.return_value = False
    ctx.bronze.archive.side_effect = lambda source, identity, content, content_type, content_hash, original_filename, metadata: f"s3://chip-bronze/{source}/{identity}/{content_hash}/{original_filename}"
    ctx.handoff.signal.return_value = 1
    ctx.handoff.get_registered_extractors.return_value = ["ajk_idsrs_disease_tables"]

    summary = run_connector(conn, ctx)
    assert summary.discovered == 3
    assert summary.fetched == 3
    assert summary.archived == 3
    assert summary.errors == 0


def test_e2e_pitb_dss_pipeline():
    conn = PitbDssConnector(source_dir="Data_sources_1/PITB-DSS")
    ctx = MagicMock()
    ctx.dedup.identity_seen.return_value = False
    ctx.dedup.content_seen.return_value = False
    ctx.bronze.archive.side_effect = lambda source, identity, content, content_type, content_hash, original_filename, metadata: f"s3://chip-bronze/{source}/{identity}/{content_hash}/{original_filename}"
    ctx.handoff.signal.return_value = 1
    ctx.handoff.get_registered_extractors.return_value = ["pitb_dss_disease_tables"]

    summary = run_connector(conn, ctx)
    assert summary.discovered == 168
    assert summary.fetched == 168
    assert summary.archived == 168
    assert summary.errors == 0


def test_e2e_nih_idsr_pipeline():
    conn = NihIdsrConnector(source_dir="Data_sources_1/NIH/MD")
    ctx = MagicMock()
    ctx.dedup.identity_seen.return_value = False
    ctx.dedup.content_seen.return_value = False
    ctx.bronze.archive.side_effect = lambda source, identity, content, content_type, content_hash, original_filename, metadata: f"s3://chip-bronze/{source}/{identity}/{content_hash}/{original_filename}"
    ctx.handoff.signal.return_value = 1
    ctx.handoff.get_registered_extractors.return_value = ["nih_idsr_disease_tables"]

    summary = run_connector(conn, ctx)
    assert summary.discovered == 174
    assert summary.fetched == 174
    assert summary.archived == 174
    assert summary.errors == 0


def test_e2e_dhis_weekly_pipeline():
    conn = DhisPunjabWeeklyConnector(source_dir="Data_sources_1/DHIS/MD")
    ctx = MagicMock()
    ctx.dedup.identity_seen.return_value = False
    ctx.dedup.content_seen.return_value = False
    ctx.bronze.archive.side_effect = lambda source, identity, content, content_type, content_hash, original_filename, metadata: f"s3://chip-bronze/{source}/{identity}/{content_hash}/{original_filename}"
    ctx.handoff.signal.return_value = 1
    ctx.handoff.get_registered_extractors.return_value = ["dhis_punjab_disease_tables"]

    summary = run_connector(conn, ctx)
    assert summary.discovered > 0
    assert summary.errors == 0
