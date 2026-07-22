from unittest.mock import MagicMock
from ajk_idsrs_disease_tables.extractor import AjkIdsrsDiseaseTableExtractor
from dhis_punjab_disease_tables.extractor import DhisPunjabDiseaseTableExtractor
from nih_idsr_disease_tables.extractor import NihIdsrDiseaseTableExtractor
from pitb_dss_disease_tables.extractor import PitbDssDiseaseTableExtractor
from chip_extractors.runner import run_extractor


def test_layer3_ajk_extraction_runner():
    extractor = AjkIdsrsDiseaseTableExtractor()

    dummy_input = MagicMock()
    dummy_input.id = 1
    dummy_input.source = "ajk_idsrs"
    dummy_input.identity = "ajk_idsrs:2026:W18"
    dummy_input.bronze_uri = "s3://chip-bronze/..."

    ctx = MagicMock()
    ctx.handoff.poll_pending.return_value = [dummy_input]
    sample_path = "Data_sources_1/AJK/out/MD/IDSR-WEEKLY-BULLETIN-AJK_EPI-Week-18_2026.md"
    with open(sample_path, "rb") as f:
        ctx.bronze.get.return_value = f.read()

    summary = run_extractor(extractor, ctx)
    assert summary.documents_read == 1
    assert summary.documents_extracted == 1
    assert summary.records_produced > 0
    assert summary.errors == 0
    assert ctx.kafka.produce_cloudevent.call_count == summary.records_produced


def test_layer3_nih_extraction_runner():
    extractor = NihIdsrDiseaseTableExtractor()

    dummy_input = MagicMock()
    dummy_input.id = 2
    dummy_input.source = "nih_idsr"
    dummy_input.identity = "idsr:2025:W01"
    dummy_input.bronze_uri = "s3://chip-bronze/..."

    ctx = MagicMock()
    ctx.handoff.poll_pending.return_value = [dummy_input]
    sample_path = "Data_sources_1/NIH/MD/Week-01-2025.md"
    with open(sample_path, "rb") as f:
        ctx.bronze.get.return_value = f.read()

    summary = run_extractor(extractor, ctx)
    assert summary.documents_read == 1
    assert summary.documents_extracted == 1
    assert summary.records_produced > 0
    assert summary.errors == 0


def test_layer3_pitb_extraction_runner():
    extractor = PitbDssDiseaseTableExtractor()

    dummy_input = MagicMock()
    dummy_input.id = 3
    dummy_input.source = "pitb_dss"
    dummy_input.identity = "pitb_dss:2015:W11"
    dummy_input.bronze_uri = "s3://chip-bronze/..."

    ctx = MagicMock()
    ctx.handoff.poll_pending.return_value = [dummy_input]
    sample_path = "Data_sources_1/PITB-DSS/2015/MD/DSS-Bulletin-Week-11.md"
    with open(sample_path, "rb") as f:
        ctx.bronze.get.return_value = f.read()

    summary = run_extractor(extractor, ctx)
    assert summary.documents_read == 1
    assert summary.documents_extracted == 1
    assert summary.records_produced > 0
    assert summary.errors == 0


def test_layer3_dhis_extraction_runner():
    extractor = DhisPunjabDiseaseTableExtractor()

    dummy_input = MagicMock()
    dummy_input.id = 4
    dummy_input.source = "dhis_punjab_weekly"
    dummy_input.identity = "dhis_punjab_weekly:2022:W18"
    dummy_input.bronze_uri = "s3://chip-bronze/..."

    ctx = MagicMock()
    ctx.handoff.poll_pending.return_value = [dummy_input]
    sample_path = "Data_sources_1/DHIS/MD/week_18.md"
    with open(sample_path, "rb") as f:
        ctx.bronze.get.return_value = f.read()

    summary = run_extractor(extractor, ctx)
    assert summary.documents_read == 1
    assert summary.documents_extracted == 1
    assert summary.records_produced > 0
    assert summary.errors == 0
