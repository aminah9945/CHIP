from __future__ import annotations

from dagster import asset
from chip_extractors.runner import run_extractor
from ajk_idsrs_disease_tables.extractor import AjkIdsrsDiseaseTableExtractor
from dhis_punjab_disease_tables.extractor import DhisPunjabDiseaseTableExtractor
from nih_idsr_disease_tables.extractor import NihIdsrDiseaseTableExtractor
from pitb_dss_disease_tables.extractor import PitbDssDiseaseTableExtractor
from pipelines.extraction.resources import ExtractorContextResource
from pipelines.ingestion.assets import raw_ajk_idsrs, raw_dhis_punjab_weekly, raw_nih_idsr, raw_pitb_dss


@asset(group_name="extraction", kinds={"kafka", "postgres", "minio"}, deps=[raw_nih_idsr])
def extracted_nih_idsr(context, extractor_context: ExtractorContextResource):
    """Dagster extraction asset for NIH IDSR disease tables."""
    extractor = NihIdsrDiseaseTableExtractor()
    run_ctx = extractor_context.build_context(extractor_name=extractor.name)
    summary = run_extractor(extractor, run_ctx)

    context.add_output_metadata({
        "documents_read": summary.documents_read,
        "documents_extracted": summary.documents_extracted,
        "records_produced": summary.records_produced,
        "errors": summary.errors,
        "duration_ms": summary.duration_ms,
    })
    return summary


@asset(group_name="extraction", kinds={"kafka", "postgres", "minio"}, deps=[raw_pitb_dss])
def extracted_pitb_dss(context, extractor_context: ExtractorContextResource):
    """Dagster extraction asset for PITB-DSS disease tables."""
    extractor = PitbDssDiseaseTableExtractor()
    run_ctx = extractor_context.build_context(extractor_name=extractor.name)
    summary = run_extractor(extractor, run_ctx)

    context.add_output_metadata({
        "documents_read": summary.documents_read,
        "documents_extracted": summary.documents_extracted,
        "records_produced": summary.records_produced,
        "errors": summary.errors,
        "duration_ms": summary.duration_ms,
    })
    return summary


@asset(group_name="extraction", kinds={"kafka", "postgres", "minio"}, deps=[raw_ajk_idsrs])
def extracted_ajk_idsrs(context, extractor_context: ExtractorContextResource):
    """Dagster extraction asset for AJK IDSRS disease tables."""
    extractor = AjkIdsrsDiseaseTableExtractor()
    run_ctx = extractor_context.build_context(extractor_name=extractor.name)
    summary = run_extractor(extractor, run_ctx)

    context.add_output_metadata({
        "documents_read": summary.documents_read,
        "documents_extracted": summary.documents_extracted,
        "records_produced": summary.records_produced,
        "errors": summary.errors,
        "duration_ms": summary.duration_ms,
    })
    return summary


@asset(group_name="extraction", kinds={"kafka", "postgres", "minio"}, deps=[raw_dhis_punjab_weekly])
def extracted_dhis_punjab_weekly(context, extractor_context: ExtractorContextResource):
    """Dagster extraction asset for DHIS Punjab disease tables."""
    extractor = DhisPunjabDiseaseTableExtractor()
    run_ctx = extractor_context.build_context(extractor_name=extractor.name)
    summary = run_extractor(extractor, run_ctx)

    context.add_output_metadata({
        "documents_read": summary.documents_read,
        "documents_extracted": summary.documents_extracted,
        "records_produced": summary.records_produced,
        "errors": summary.errors,
        "duration_ms": summary.duration_ms,
    })
    return summary
