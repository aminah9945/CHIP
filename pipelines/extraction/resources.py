from __future__ import annotations

import os
from dagster import ConfigurableResource
from chip_connectors.bronze import BronzeClient
from chip_extractors.base import ExtractorContext
from chip_extractors.handoff import HandoffClient
from chip_extractors.kafka_producer import KafkaProducerClient
from chip_extractors.logging import get_extractor_logger
from chip_extractors.metrics import MetricsEmitter
from chip_extractors.table_parser import TableParser


class ExtractorContextResource(ConfigurableResource):
    """Dagster resource that builds the ExtractorContext for extractor execution."""

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")
    minio_secure: bool = False
    minio_bucket: str = "chip-bronze"

    db_url: str = os.getenv("DATABASE_URL", "postgresql://chip:chip_password@localhost:5432/chip")
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")

    def build_context(self, extractor_name: str) -> ExtractorContext:
        """Create an ExtractorContext instance configured for a specific extractor."""
        bronze = BronzeClient(
            endpoint=self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            bucket=self.minio_bucket,
            secure=self.minio_secure,
        )
        handoff = HandoffClient(db_url_or_conn=self.db_url)
        kafka = KafkaProducerClient(bootstrap_servers=self.kafka_bootstrap_servers)
        table_parser = TableParser()
        logger = get_extractor_logger(extractor=extractor_name)
        metrics = MetricsEmitter(db_url_or_conn=self.db_url)

        return ExtractorContext(
            bronze=bronze,
            handoff=handoff,
            kafka=kafka,
            table_parser=table_parser,
            log=logger,
            metrics=metrics,
        )
