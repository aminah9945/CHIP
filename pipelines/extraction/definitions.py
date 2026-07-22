from __future__ import annotations

from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from pipelines.extraction import assets as extraction_assets, checks as extraction_checks
from pipelines.extraction.resources import ExtractorContextResource
from pipelines.ingestion import assets as ingestion_assets, checks as ingestion_checks
from pipelines.ingestion.resources import ConnectorContextResource

all_assets = load_assets_from_modules([ingestion_assets, extraction_assets])
all_checks = load_asset_checks_from_modules([ingestion_checks, extraction_checks])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources={
        "connector_context": ConnectorContextResource(),
        "extractor_context": ExtractorContextResource(),
    },
)
