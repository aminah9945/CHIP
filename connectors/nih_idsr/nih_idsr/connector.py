from __future__ import annotations

import os
from pathlib import Path
import re
from typing import Iterator
from datetime import datetime, timezone
from chip_connectors.base import Connector, DiscoveredItem, RawArtifact, RunContext


class NihIdsrConnector(Connector):
    """Thin connector for NIH IDSR weekly public health bulletins (2021-2026)."""

    name = "nih_idsr"
    connector_version = "1.0.0"
    content_type = "text/markdown"

    PATTERNS = [
        # Week-01-2025.md
        re.compile(r"^Week-(\d+)-(\d{4})$", re.IGNORECASE),
        # Weekly Report-01-2024.md / Weekly_Report_22_2023.md
        re.compile(r"^Weekly[_ ]Report[-_](\d+)[-_](\d{4})$", re.IGNORECASE),
        # IDSR-Weekly-Report-11-2022.md
        re.compile(r"^IDSR-Weekly-Report-(\d+)-(\d{4})$", re.IGNORECASE),
        # IDSRS Weekly Report-01-2026 / IDSRS Weekly Report 11-2026
        re.compile(r"^IDSRS Weekly Report[- ]?(\d+)-(\d{4})$", re.IGNORECASE),
        # IDSR Week 13 Bulletin (2025).md
        re.compile(r"^IDSR Week (\d+) Bulletin \((\d{4})\)$", re.IGNORECASE),
    ]

    YEARLESS_PATTERN = re.compile(r"^IDSR-Weekly-Report-(\d+)$", re.IGNORECASE)

    def __init__(self, source_dir: str | Path = "Data_sources_1/NIH/MD") -> None:
        self.source_dir = Path(source_dir)

    def clean_filename(self, filename: str) -> str:
        """Strip extension and trailing revision/update suffixes from filename."""
        name = filename
        if name.endswith(".md"):
            name = name[:-3]

        # Strip trailing update tags (e.g. -updated-NG, -NEW, -updated (1), -Final)
        name = re.sub(r"[-_ ]*(updated|NEW|NG|Final)[-_ \(\)\w]*$", "", name, flags=re.IGNORECASE).strip()
        # Strip trailing revision numbers attached to year (e.g. 2022-1 -> 2022)
        name = re.sub(r"(\d{4})[-_]\d+$", r"\1", name).strip()
        # Strip trailing parenthetical revision numbers like (1) ONLY if preceded by year/bulletin
        name = re.sub(r"(?<=\d{4})\s*\(\d+\)$", "", name).strip()

        return name

    def extract_week_year_from_content(self, file_path: Path) -> tuple[int | None, int | None]:
        """Scan first 50 lines of an NIH markdown file for week and year."""
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = [f.readline() for _ in range(50)]
            text = "\n".join(lines)

            # Year search (2021-2026)
            year_match = re.search(r"\b(202[1-6])\b", text)
            year = int(year_match.group(1)) if year_match else None

            # Week search: "Highlights of the week 21" or "during week 21" or "Epi Week 21"
            week_match = re.search(r"\b(?:week|wk)[_\s-]+(\d{1,2})\b", text, re.IGNORECASE)
            week = int(week_match.group(1)) if week_match else None

            return week, year
        except Exception:
            return None, None

    def derive_identity(self, source_uri: str, ctx: RunContext | None = None) -> str:
        """Derive identity key 'idsr:{year}:W{week:02d}' from URI/filename + content fallback."""
        filename = os.path.basename(source_uri)
        clean = self.clean_filename(filename)

        # 1. Try standard regex patterns on clean name
        for pattern in self.PATTERNS:
            match = pattern.search(clean)
            if match:
                week = int(match.group(1))
                year = int(match.group(2))
                return f"idsr:{year}:W{week:02d}"

        # 2. Try yearless pattern + content fallback for year
        match = self.YEARLESS_PATTERN.search(clean)
        path = Path(source_uri)
        if match:
            week = int(match.group(1))
            year = None
            if path.exists() and path.is_file():
                _, year = self.extract_week_year_from_content(path)
            if year:
                return f"idsr:{year}:W{week:02d}"

        # 3. Full content fallback (e.g. IDSR-Weekly-Report-21-June-2021.md)
        if path.exists() and path.is_file():
            c_week, c_year = self.extract_week_year_from_content(path)
            if c_week and c_year:
                return f"idsr:{c_year}:W{c_week:02d}"

        raise ValueError(f"Could not derive identity for NIH file: {filename}")

    def discover(self, ctx: RunContext) -> Iterator[DiscoveredItem]:
        """Scan NIH MD directory for candidate .md files."""
        if not self.source_dir.exists():
            if ctx.log:
                ctx.log.warn("nih_idsr.discover.dir_not_found", path=str(self.source_dir))
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
                    ctx.log.warn("nih_idsr.discover.skip_unparseable", file=path.name, error=str(e))
                continue

    def fetch(self, item: DiscoveredItem, ctx: RunContext) -> RawArtifact:
        """Read raw bytes for one NIH MD file."""
        path = Path(item.source_uri)
        content = path.read_bytes()
        return RawArtifact(
            item=item,
            content=content,
            content_type=self.content_type,
            fetched_at=datetime.now(timezone.utc),
            original_filename=path.name,
        )
