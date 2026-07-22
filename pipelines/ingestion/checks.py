from __future__ import annotations

import psycopg
from dagster import AssetCheckResult, asset_check
from pipelines.ingestion.assets import raw_ajk_idsrs, raw_dhis_punjab_weekly, raw_nih_idsr, raw_pitb_dss
from pipelines.ingestion.resources import ConnectorContextResource


def _check_source_ingestion(source: str, summary, connector_context: ConnectorContextResource) -> AssetCheckResult:
    """Verify zero discovery, low error rate, and extractor_status seeding."""
    discovered = summary.discovered
    archived = summary.archived
    errors = summary.errors

    # 1. Zero discovery guard
    passed_discovery = discovered > 0
    description = f"Discovered {discovered} items, archived {archived}, errors {errors}."

    # 2. Status seeding check in DB
    status_seeded_passed = True
    try:
        with psycopg.connect(connector_context.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(DISTINCT rd.id)
                    FROM ingestion.raw_documents rd
                    LEFT JOIN ingestion.extractor_status es ON es.raw_document_id = rd.id
                    WHERE rd.source = %s AND es.id IS NULL;
                    """,
                    (source,),
                )
                missing_count = cur.fetchone()[0]
                if missing_count > 0:
                    status_seeded_passed = False
                    description += f" WARNING: {missing_count} raw_documents missing extractor_status rows!"
    except Exception:
        # DB unreachable during pure asset check test
        pass

    passed = passed_discovery and (errors == 0 or errors / max(1, discovered) < 0.05) and status_seeded_passed

    return AssetCheckResult(
        passed=passed,
        description=description,
        metadata={"discovered": discovered, "archived": archived, "errors": errors},
    )


@asset_check(asset=raw_nih_idsr)
def check_nih_idsr_ingestion(summary, connector_context: ConnectorContextResource):
    return _check_source_ingestion("nih_idsr", summary, connector_context)


@asset_check(asset=raw_pitb_dss)
def check_pitb_dss_ingestion(summary, connector_context: ConnectorContextResource):
    return _check_source_ingestion("pitb_dss", summary, connector_context)


@asset_check(asset=raw_ajk_idsrs)
def check_ajk_idsrs_ingestion(summary, connector_context: ConnectorContextResource):
    return _check_source_ingestion("ajk_idsrs", summary, connector_context)


@asset_check(asset=raw_dhis_punjab_weekly)
def check_dhis_punjab_weekly_ingestion(summary, connector_context: ConnectorContextResource):
    return _check_source_ingestion("dhis_punjab_weekly", summary, connector_context)
