from __future__ import annotations

from pathlib import Path
from pydantic import BaseModel, Field
import yaml


class KafkaConfig(BaseModel):
    topic: str
    bootstrap_servers: str = "localhost:9094"


class ExtractorConfig(BaseModel):
    extractor_name: str
    extractor_version: str = "1.0.0"
    kafka: KafkaConfig
    input_sources: list[str] = Field(default_factory=list)

    @classmethod
    def load_from_yaml(cls, path: str | Path) -> ExtractorConfig:
        """Load and validate extractor config from a YAML file."""
        yaml_path = Path(path)
        if not yaml_path.exists():
            raise FileNotFoundError(f"Config file not found: {yaml_path}")

        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return cls.model_validate(data)
