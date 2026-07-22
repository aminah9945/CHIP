from __future__ import annotations

from datetime import datetime, timezone
import time
from chip_connectors.base import Connector, RawDocumentRow, RunContext, RunSummary


def run_connector(conn: Connector, ctx: RunContext) -> RunSummary:
    """The template algorithm executed by every connector.

    Discovers candidate items, applies identity and content deduplication,
    archives raw content to MinIO bronze storage, signals raw_documents in Postgres,
    and seeds extractor_status for registered extractors.
    """
    summary = RunSummary(source=conn.name)
    started_at = datetime.now(timezone.utc)
    start_time_perf = time.perf_counter()

    if ctx.log:
        ctx.log.info("connector.run.started", source=conn.name)

    try:
        discovered_items = list(conn.discover(ctx))
    except Exception as e:
        if ctx.log:
            ctx.log.error("connector.discover.failed", source=conn.name, error=str(e))
        summary.errors += 1
        summary.duration_ms = int((time.perf_counter() - start_time_perf) * 1000)
        if ctx.metrics:
            ctx.metrics.emit(summary, connector_version=conn.connector_version, started_at=started_at)
        return summary

    for item in discovered_items:
        summary.discovered += 1

        # ── Step 1: Identity dedup check ──────────────────────────────────
        try:
            if ctx.dedup.identity_seen(conn.name, item.identity):
                if ctx.log:
                    ctx.log.debug("connector.dedup.identity_skip", identity=item.identity)
                summary.skipped_identity += 1
                continue
        except Exception as e:
            if ctx.log:
                ctx.log.error("connector.dedup.identity_check_failed", identity=item.identity, error=str(e))
            summary.errors += 1
            continue

        # ── Step 2: Fetch raw bytes ─────────────────────────────────────────
        try:
            raw = conn.fetch(item, ctx)
            summary.fetched += 1
        except Exception as e:
            if ctx.log:
                ctx.log.error("connector.fetch.failed", identity=item.identity, error=str(e))
            summary.errors += 1
            continue

        # ── Step 3: Content dedup check (before archiving) ─────────────────
        try:
            if ctx.dedup.content_seen(conn.name, raw.content_hash):
                if ctx.log:
                    ctx.log.debug("connector.dedup.content_skip", identity=item.identity, hash=raw.content_hash)
                summary.skipped_content += 1
                continue
        except Exception as e:
            if ctx.log:
                ctx.log.error("connector.dedup.content_check_failed", identity=item.identity, error=str(e))
            summary.errors += 1
            continue

        # ── Step 4: Archive to bronze (MinIO) ──────────────────────────────
        try:
            bronze_uri = ctx.bronze.archive(
                source=conn.name,
                identity=item.identity,
                content=raw.content,
                content_type=raw.content_type,
                content_hash=raw.content_hash,
                original_filename=raw.original_filename,
                metadata={
                    "source_uri": item.source_uri,
                    "retrieved_at": raw.fetched_at.isoformat(),
                    "connector_version": conn.connector_version,
                },
            )
            summary.archived += 1
        except Exception as e:
            if ctx.log:
                ctx.log.error("connector.archive.failed", identity=item.identity, error=str(e))
            summary.errors += 1
            continue

        # ── Step 5: Record content hash ────────────────────────────────────
        try:
            ctx.dedup.record_content(conn.name, raw.content_hash)
        except Exception as e:
            if ctx.log:
                ctx.log.error("connector.dedup.record_content_failed", identity=item.identity, error=str(e))

        # ── Step 6 & 7: Signal downstream + seed extractor status ──────────
        try:
            row = RawDocumentRow(
                source=conn.name,
                identity=item.identity,
                bronze_uri=bronze_uri,
                content_hash=raw.content_hash,
                content_type=raw.content_type,
                original_filename=raw.original_filename,
                source_uri=item.source_uri,
                connector_version=conn.connector_version,
                retrieved_at=raw.fetched_at,
                file_size_bytes=len(raw.content),
            )
            doc_id = ctx.handoff.signal(row)
            ctx.dedup.record_identity(conn.name, item.identity, raw.content_hash)

            # Seed extractor_status for registered extractors
            registered_extractors = ctx.handoff.get_registered_extractors(conn.name)
            for extractor_name in registered_extractors:
                ctx.handoff.seed_extractor_status(doc_id, extractor_name)

        except Exception as e:
            if ctx.log:
                ctx.log.error("connector.signal.failed", identity=item.identity, error=str(e))
            summary.errors += 1

    summary.duration_ms = int((time.perf_counter() - start_time_perf) * 1000)

    if ctx.metrics:
        try:
            ctx.metrics.emit(summary, connector_version=conn.connector_version, started_at=started_at)
        except Exception as e:
            if ctx.log:
                ctx.log.error("connector.metrics.failed", source=conn.name, error=str(e))

    if ctx.log:
        ctx.log.info("connector.run.completed", **summary.__dict__)

    return summary
