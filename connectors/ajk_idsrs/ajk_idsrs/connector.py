from __future__ import annotations

import os
from pathlib import Path
import re
from typing import Iterator
from datetime import datetime, timezone
from chip_connectors.base import Connector, DiscoveredItem, RawArtifact, RunContext


class AjkIdsrsConnector(Connector):
    """Thin connector for AJK IDSRS weekly surveillance bulletins."""

    name = "ajk_idsrs"
    connector_version = "1.0.0"
    content_type = "text/markdown"

    PATTERN = re.compile(r"IDSR-WEEKLY-BULLETIN-AJK_EPI-Week-(\d+)_(\d{4})", re.IGNORECASE)

    def __init__(self, source_dir: str | Path = "Data_sources_1/AJK/out/MD") -> None:
        self.source_dir = Path(source_dir)

    def derive_identity(self, source_uri: str, ctx: RunContext | None = None) -> str:
        """Derive identity key 'ajk_idsrs:{year}:W{week:02d}' from filename."""
        filename = os.path.basename(source_uri)
        match = self.PATTERN.search(filename)
        if not match:
            raise ValueError(f"Could not parse week/year from AJK filename: {filename}")

        week_str, year_str = match.group(1), match.group(2)
        week = int(week_str)
        year = int(year_str)
        return f"ajk_idsrs:{year}:W{week:02d}"

    def discover(self, ctx: RunContext) -> Iterator[DiscoveredItem]:
        """Scan AJK MD directory for candidate .md files."""
        if not self.source_dir.exists():
            if ctx.log:
                ctx.log.warn("ajk_idsrs.discover.dir_not_found", path=str(self.source_dir))
            return

        for path in sorted(self.source_dir.glob("*.md")):
            try:
                identity = self.derive_identity(str(path), ctx)
                yield DiscoveredItem(
                    source_uri=str(path),
                    identity=identity,
                )
            except ValueError as e:
                if ctx.log:
                    ctx.log.warn("ajk_idsrs.discover.skip_unparseable", file=path.name, error=str(e))
                continue

    def fetch(self, item: DiscoveredItem, ctx: RunContext) -> RawArtifact:
        """Read raw bytes for one AJK MD file."""
        path = Path(item.source_uri)
        content = path.read_bytes()
        return RawArtifact(
            item=item,
            content=content,
            content_type=self.content_type,
            fetched_at=datetime.now(timezone.utc),
            original_filename=path.name,
        )
