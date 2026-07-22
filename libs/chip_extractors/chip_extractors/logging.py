from __future__ import annotations

import logging
import sys
import structlog


def configure_logging(level: int = logging.INFO) -> None:
    """Configure structlog for JSON logging in Layer 3 extractors."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_extractor_logger(extractor: str, run_id: str | None = None) -> structlog.BoundLogger:
    """Get a structlog bound logger pre-populated with extractor and run_id."""
    configure_logging()
    logger = structlog.get_logger()
    context = {"extractor": extractor}
    if run_id:
        context["run_id"] = run_id
    return logger.bind(**context)
