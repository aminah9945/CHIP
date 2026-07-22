from __future__ import annotations

from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from pipelines.ingestion import assets, checks
from pipelines.ingestion.resources import ConnectorContextResource

all_assets = load_assets_from_modules([assets])
all_checks = load_asset_checks_from_modules([checks])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources={
        "connector_context": ConnectorContextResource(),
    },
)
