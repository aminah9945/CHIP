from __future__ import annotations

from dagster import AssetCheckResult, asset_check
from pipelines.extraction.assets import (
    extracted_ajk_idsrs,
    extracted_dhis_punjab_weekly,
    extracted_nih_idsr,
    extracted_pitb_dss,
)
from pipelines.extraction.resources import ExtractorContextResource


def _check_extraction(summary) -> AssetCheckResult:
    """Verify non-zero extraction and low error rate."""
    docs_read = summary.documents_read
    docs_extracted = summary.documents_extracted
    records = summary.records_produced
    errors = summary.errors

    passed = (errors == 0 or errors / max(1, docs_read) < 0.05)
    description = f"Read {docs_read} docs, extracted {docs_extracted}, produced {records} records, errors {errors}."

    return AssetCheckResult(
        passed=passed,
        description=description,
        metadata={
            "documents_read": docs_read,
            "documents_extracted": docs_extracted,
            "records_produced": records,
            "errors": errors,
        },
    )


@asset_check(asset=extracted_nih_idsr)
def check_nih_idsr_extraction(summary, extractor_context: ExtractorContextResource):
    return _check_extraction(summary)


@asset_check(asset=extracted_pitb_dss)
def check_pitb_dss_extraction(summary, extractor_context: ExtractorContextResource):
    return _check_extraction(summary)


@asset_check(asset=extracted_ajk_idsrs)
def check_ajk_idsrs_extraction(summary, extractor_context: ExtractorContextResource):
    return _check_extraction(summary)


@asset_check(asset=extracted_dhis_punjab_weekly)
def check_dhis_punjab_weekly_extraction(summary, extractor_context: ExtractorContextResource):
    return _check_extraction(summary)
