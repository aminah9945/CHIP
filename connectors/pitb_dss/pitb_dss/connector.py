from __future__ import annotations

import os
from pathlib import Path
import re
from typing import Iterator
from datetime import datetime, timezone
from chip_connectors.base import Connector, DiscoveredItem, RawArtifact, RunContext


class PitbDssConnector(Connector):
    """Thin connector for PITB-DSS / HRS surveillance bulletins (2015-2018)."""

    name = "pitb_dss"
    connector_version = "1.0.0"
    content_type = "text/markdown"

    PATTERNS = [
        # HRS or DSS Bulletin Week 11,2017 / Week-2-2016 / Week 20_2016 / HRS-Bulletin-Week-44-2016
        re.compile(r"(?:DSS|HRS)[- ]+Bulletin[- ]+Week[-_ ]+(\d+)[-_ ,]+(\d{4})", re.IGNORECASE),
        re.compile(r"HRS-Bulletin-Week-(\d+)-(\d{4})", re.IGNORECASE),
        # DSS-Bulletin-Week-11.md (Year from parent directory)
        re.compile(r"DSS-Bulletin-Week-(\d+)", re.IGNORECASE),
    ]

    def __init__(self, source_dir: str | Path = "Data_sources_1/PITB-DSS") -> None:
        self.source_dir = Path(source_dir)

    def derive_identity(self, source_uri: str, ctx: RunContext | None = None) -> str:
        """Derive identity key 'pitb_dss:{year}:W{week:02d}' from URI/filename."""
        filename = os.path.basename(source_uri)
        path = Path(source_uri)

        # Ignore junk files like _.md
        if filename in ("_.md", ".md"):
            raise ValueError(f"Junk file ignored: {filename}")

        # Try regex patterns
        for i, pattern in enumerate(self.PATTERNS):
            match = pattern.search(filename)
            if match:
                if i in (0, 1):  # Has week and year in filename
                    week = int(match.group(1))
                    year = int(match.group(2))
                    return f"pitb_dss:{year}:W{week:02d}"
                else:  # DSS-Bulletin-Week-(\d+) -> Year from parent directory
                    week = int(match.group(1))
                    year = None
                    for part in path.parts:
                        if re.match(r"^20\d{2}$", part):
                            year = int(part)
                            break
                    if year is None:
                        raise ValueError(f"Year not found in filename or parent path for {source_uri}")
                    return f"pitb_dss:{year}:W{week:02d}"

        raise ValueError(f"Could not parse PITB-DSS filename: {filename}")

    def discover(self, ctx: RunContext) -> Iterator[DiscoveredItem]:
        """Scan PITB-DSS directory for candidate .md files recursively."""
        if not self.source_dir.exists():
            if ctx.log:
                ctx.log.warn("pitb_dss.discover.dir_not_found", path=str(self.source_dir))
            return

        for path in sorted(self.source_dir.glob("**/*.md")):
            try:
                identity = self.derive_identity(str(path), ctx)
                yield DiscoveredItem(
                    source_uri=str(path),
                    identity=identity,
                )
            except ValueError as e:
                if ctx.log:
                    ctx.log.warn("pitb_dss.discover.skip_unparseable", file=path.name, error=str(e))
                continue

    def fetch(self, item: DiscoveredItem, ctx: RunContext) -> RawArtifact:
        """Read raw bytes for one PITB-DSS MD file."""
        path = Path(item.source_uri)
        content = path.read_bytes()
        return RawArtifact(
            item=item,
            content=content,
            content_type=self.content_type,
            fetched_at=datetime.now(timezone.utc),
            original_filename=path.name,
        )
