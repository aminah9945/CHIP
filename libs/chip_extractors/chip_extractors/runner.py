from __future__ import annotations

from datetime import datetime, timezone
import time
from chip_extractors.base import Extractor, ExtractorContext, RunSummary


def run_extractor(extractor: Extractor, ctx: ExtractorContext, limit: int = 100) -> RunSummary:
    """The template algorithm executed by every extractor.

    Polls pending raw_documents, sets status='extracting', fetches bronze bytes,
    executes extractor.extract(), publishes CloudEvents to Kafka, and updates
    status='extracted' (or 'failed' on error).
    """
    summary = RunSummary(extractor=extractor.name)
    start_time_perf = time.perf_counter()

    if ctx.log:
        ctx.log.info("extractor.run.started", extractor=extractor.name)

    try:
        pending_items = ctx.handoff.poll_pending(extractor.name, limit=limit)
    except Exception as e:
        if ctx.log:
            ctx.log.error("extractor.poll_pending.failed", extractor=extractor.name, error=str(e))
        summary.errors += 1
        summary.duration_ms = int((time.perf_counter() - start_time_perf) * 1000)
        return summary

    for inp in pending_items:
        summary.documents_read += 1

        # ── Step 1: Mark status = 'extracting' ─────────────────────────────
        try:
            ctx.handoff.update_status(inp.id, extractor.name, "extracting")
        except Exception as e:
            if ctx.log:
                ctx.log.error("extractor.update_status.extracting_failed", identity=inp.identity, error=str(e))
            summary.errors += 1
            continue

        # ── Step 2: Fetch raw content from MinIO Bronze ─────────────────────
        try:
            content = ctx.bronze.get(inp.bronze_uri)
        except Exception as e:
            if ctx.log:
                ctx.log.error("extractor.fetch_bronze.failed", identity=inp.identity, uri=inp.bronze_uri, error=str(e))
            ctx.handoff.update_status(
                inp.id, extractor.name, "failed", error_message=f"Failed to fetch bronze object: {e}"
            )
            summary.errors += 1
            continue

        # ── Step 3 & 4: Extract records & publish CloudEvents to Kafka ───────
        try:
            records = list(extractor.extract(inp, content, ctx))
            for record in records:
                ctx.kafka.produce_cloudevent(extractor.kafka_topic, record, inp, extractor.name)

            ctx.kafka.flush()

            # ── Step 5: Mark status = 'extracted' ───────────────────────────
            ctx.handoff.update_status(
                inp.id, extractor.name, "extracted", records_produced=len(records)
            )
            summary.documents_extracted += 1
            summary.records_produced += len(records)

            if ctx.log:
                ctx.log.debug("extractor.document.extracted", identity=inp.identity, records=len(records))

        except Exception as e:
            if ctx.log:
                ctx.log.error("extractor.extract.failed", identity=inp.identity, error=str(e))
            ctx.handoff.update_status(
                inp.id, extractor.name, "failed", error_message=str(e)
            )
            summary.errors += 1

    summary.duration_ms = int((time.perf_counter() - start_time_perf) * 1000)

    if ctx.log:
        ctx.log.info("extractor.run.completed", **summary.__dict__)

    return summary
