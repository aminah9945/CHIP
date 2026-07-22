from __future__ import annotations

import os
from pathlib import Path
import re
from typing import Iterator
from datetime import datetime, timezone
from chip_connectors.base import Connector, DiscoveredItem, RawArtifact, RunContext


class DhisPunjabWeeklyConnector(Connector):
    """Thin connector for DHIS Punjab Weekly feedback bulletins (DHIS-II era, 2022)."""

    name = "dhis_punjab_weekly"
    connector_version = "1.0.0"
    content_type = "text/markdown"

    WEEK_PATTERN = re.compile(r"^[Ww]eek[_ ]?(\d+)", re.IGNORECASE)

    def __init__(self, source_dir: str | Path = "Data_sources_1/DHIS/MD") -> None:
        self.source_dir = Path(source_dir)

    def extract_year_from_content(self, file_path: Path) -> int | None:
        """Scan first 30 lines of a DHIS markdown file to extract reporting year."""
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = [f.readline() for _ in range(30)]
            text = "\n".join(lines)

            # Era hint checks
            if "DHIS-II" in text:
                return 2022
            if "DHIS2" in text or "DHIS-2" in text:
                # DHIS2 era (2024-2025)
                # If explicit 2024/2025 found, return it so caller can filter out
                match = re.search(r"\b(202[4-9])\b", text)
                if match:
                    return int(match.group(1))
                return 2024

            # Search 4-digit years in 2010-2029 range
            match = re.search(r"\b(20[1-2][0-9])\b", text)
            if match:
                return int(match.group(1))

            return None
        except Exception:
            return None

    def derive_identity(self, source_uri: str, ctx: RunContext | None = None, year_hint: int | None = None) -> str:
        """Derive identity key 'dhis_punjab_weekly:{year}:W{week:02d}' from URI/filename + content year."""
        filename = os.path.basename(source_uri)

        # Strip parenthetical suffixes like (1), (2), (3)
        clean_name = re.sub(r"\(\d+\)", "", filename).strip()

        match = self.WEEK_PATTERN.match(clean_name)
        if not match:
            raise ValueError(f"Filename does not match week pattern: {filename}")

        week = int(match.group(1))

        year = year_hint
        if year is None:
            path = Path(source_uri)
            if path.exists() and path.is_file():
                year = self.extract_year_from_content(path)

        if year is None:
            raise ValueError(f"Could not determine year for DHIS file: {filename}")

        return f"dhis_punjab_weekly:{year}:W{week:02d}"

    def discover(self, ctx: RunContext) -> Iterator[DiscoveredItem]:
        """Scan DHIS MD directory for weekly .md files belonging to 2022 (DHIS-II era)."""
        if not self.source_dir.exists():
            if ctx.log:
                ctx.log.warn("dhis_punjab_weekly.discover.dir_not_found", path=str(self.source_dir))
            return

        for path in sorted(self.source_dir.glob("*.md")):
            filename = path.name

            # Silent skip for non-weekly files (Annual_Report, Analysis, etc.)
            clean_name = re.sub(r"\(\d+\)", "", filename).strip()
            if not self.WEEK_PATTERN.match(clean_name):
                continue

            year = self.extract_year_from_content(path)
            # Only ingest 2022 (DHIS-II era weekly bulletins)
            if year != 2022:
                if ctx.log:
                    ctx.log.debug("dhis_punjab_weekly.discover.skip_era", file=filename, year=year)
                continue

            try:
                identity = self.derive_identity(str(path), ctx, year_hint=year)
                yield DiscoveredItem(
                    source_uri=str(path),
                    identity=identity,
                    hints={"year": year},
                )
            except ValueError as e:
                if ctx.log:
                    ctx.log.warn("dhis_punjab_weekly.discover.skip_unparseable", file=filename, error=str(e))
                continue

    def fetch(self, item: DiscoveredItem, ctx: RunContext) -> RawArtifact:
        """Read raw bytes for one DHIS MD file."""
        path = Path(item.source_uri)
        content = path.read_bytes()
        return RawArtifact(
            item=item,
            content=content,
            content_type=self.content_type,
            fetched_at=datetime.now(timezone.utc),
            original_filename=path.name,
        )
