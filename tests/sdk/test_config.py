import tempfile
from pathlib import Path
import pytest
from chip_connectors.config import ConnectorConfig


def test_load_valid_yaml(tmp_path: Path):
    yaml_content = """
source: nih_idsr
connector_version: "1.0.0"
content_type: text/markdown
discovery:
  source_directory: Data_sources_1/NIH/MD
  file_extension: .md
schedule:
  cron: "0 6 * * 2"
  timezone: Asia/Karachi
freshness_max_lag_hours: 192
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml_content)

    cfg = ConnectorConfig.load_from_yaml(config_file)
    assert cfg.source == "nih_idsr"
    assert cfg.connector_version == "1.0.0"
    assert cfg.discovery.source_directory == "Data_sources_1/NIH/MD"
    assert cfg.discovery.file_extension == ".md"
    assert cfg.schedule.cron == "0 6 * * 2"


def test_missing_file():
    with pytest.raises(FileNotFoundError):
        ConnectorConfig.load_from_yaml("non_existent_config.yaml")
