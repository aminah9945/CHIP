from __future__ import annotations

from dagster import AssetExecutionContext, asset
from chip_connectors.runner import run_connector
from ajk_idsrs.connector import AjkIdsrsConnector
from dhis_punjab_weekly.connector import DhisPunjabWeeklyConnector
from nih_idsr.connector import NihIdsrConnector
from pitb_dss.connector import PitbDssConnector
from pipelines.ingestion.resources import ConnectorContextResource


@asset(group_name="ingestion", kinds={"minio", "postgres"})
def raw_nih_idsr(context, connector_context: ConnectorContextResource):
    """Dagster asset for NIH IDSR weekly public health bulletins."""
    conn = NihIdsrConnector()
    run_ctx = connector_context.build_context(source=conn.name)
    summary = run_connector(conn, run_ctx)

    context.add_output_metadata({
        "discovered": summary.discovered,
        "fetched": summary.fetched,
        "archived": summary.archived,
        "skipped_identity": summary.skipped_identity,
        "skipped_content": summary.skipped_content,
        "errors": summary.errors,
        "duration_ms": summary.duration_ms,
    })
    return summary


@asset(group_name="ingestion", kinds={"minio", "postgres"})
def raw_pitb_dss(context, connector_context: ConnectorContextResource):
    """Dagster asset for PITB-DSS / HRS disease surveillance bulletins."""
    conn = PitbDssConnector()
    run_ctx = connector_context.build_context(source=conn.name)
    summary = run_connector(conn, run_ctx)

    context.add_output_metadata({
        "discovered": summary.discovered,
        "fetched": summary.fetched,
        "archived": summary.archived,
        "skipped_identity": summary.skipped_identity,
        "skipped_content": summary.skipped_content,
        "errors": summary.errors,
        "duration_ms": summary.duration_ms,
    })
    return summary


@asset(group_name="ingestion", kinds={"minio", "postgres"})
def raw_ajk_idsrs(context, connector_context: ConnectorContextResource):
    """Dagster asset for AJK IDSRS weekly surveillance bulletins."""
    conn = AjkIdsrsConnector()
    run_ctx = connector_context.build_context(source=conn.name)
    summary = run_connector(conn, run_ctx)

    context.add_output_metadata({
        "discovered": summary.discovered,
        "fetched": summary.fetched,
        "archived": summary.archived,
        "skipped_identity": summary.skipped_identity,
        "skipped_content": summary.skipped_content,
        "errors": summary.errors,
        "duration_ms": summary.duration_ms,
    })
    return summary


@asset(group_name="ingestion", kinds={"minio", "postgres"})
def raw_dhis_punjab_weekly(context, connector_context: ConnectorContextResource):
    """Dagster asset for DHIS Punjab weekly feedback bulletins."""
    conn = DhisPunjabWeeklyConnector()
    run_ctx = connector_context.build_context(source=conn.name)
    summary = run_connector(conn, run_ctx)

    context.add_output_metadata({
        "discovered": summary.discovered,
        "fetched": summary.fetched,
        "archived": summary.archived,
        "skipped_identity": summary.skipped_identity,
        "skipped_content": summary.skipped_content,
        "errors": summary.errors,
        "duration_ms": summary.duration_ms,
    })
    return summary
