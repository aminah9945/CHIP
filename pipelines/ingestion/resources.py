from __future__ import annotations

import os
from dagster import ConfigurableResource
from chip_connectors.base import RunContext
from chip_connectors.bronze import BronzeClient
from chip_connectors.dedup import DedupStore
from chip_connectors.handoff import HandoffStore
from chip_connectors.logging import get_connector_logger
from chip_connectors.metrics import MetricsEmitter


class ConnectorContextResource(ConfigurableResource):
    """Dagster resource that builds the SDK RunContext for connector execution."""

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")
    minio_secure: bool = False
    minio_bucket: str = "chip-bronze"

    db_url: str = os.getenv("DATABASE_URL", "postgresql://chip:chip_password@localhost:5432/chip")

    def build_context(self, source: str) -> RunContext:
        """Create a RunContext instance configured for a specific connector source."""
        bronze = BronzeClient(
            endpoint=self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            bucket=self.minio_bucket,
            secure=self.minio_secure,
        )
        dedup = DedupStore(db_url_or_conn=self.db_url)
        handoff = HandoffStore(db_url_or_conn=self.db_url)
        logger = get_connector_logger(source=source)
        metrics = MetricsEmitter(db_url_or_conn=self.db_url)

        return RunContext(
            bronze=bronze,
            dedup=dedup,
            handoff=handoff,
            log=logger,
            metrics=metrics,
        )
