from __future__ import annotations

from pathlib import Path
from typing import Any
from pydantic import BaseModel, Field
import yaml


class DiscoveryConfig(BaseModel):
    source_directory: str
    file_extension: str = ".md"
    recursive: bool = False
    identity_patterns: list[dict[str, Any]] = Field(default_factory=list)
    content_fallback: bool = False
    content_fallback_scan_lines: int = 50


class ScheduleConfig(BaseModel):
    cron: str = "0 6 * * 2"
    timezone: str = "Asia/Karachi"


class ConnectorConfig(BaseModel):
    source: str
    connector_version: str = "1.0.0"
    content_type: str = "text/markdown"
    discovery: DiscoveryConfig
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)
    freshness_max_lag_hours: int = 192

    @classmethod
    def load_from_yaml(cls, path: str | Path) -> ConnectorConfig:
        """Load and validate connector config from a YAML file."""
        yaml_path = Path(path)
        if not yaml_path.exists():
            raise FileNotFoundError(f"Config file not found: {yaml_path}")

        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return cls.model_validate(data)
