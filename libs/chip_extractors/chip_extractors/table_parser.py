from __future__ import annotations

from dataclasses import dataclass, field
import re
from bs4 import BeautifulSoup


@dataclass
class ParsedTable:
    """Structured representation of an HTML table extracted from Markdown."""

    headers: list[str]
    rows: list[list[str]]
    caption: str = ""

    def is_empty(self) -> bool:
        return len(self.headers) == 0 and len(self.rows) == 0


class TableParser:
    """BeautifulSoup4-based HTML table engine that parses and stitches tables."""

    def __init__(self, parser_engine: str = "lxml") -> None:
        self.parser_engine = parser_engine

    def _clean_text(self, text: str) -> str:
        """Strip HTML formatting tags, sub/sup tags, and normalize whitespace."""
        if not text:
            return ""
        # Remove superscripts/subscripts like 3rd, 5th, etc
        cleaned = re.sub(r"<sup.*?>.*?</sup>", "", text, flags=re.IGNORECASE)
        cleaned = re.sub(r"<sub.*?>.*?</sub>", "", cleaned, flags=re.IGNORECASE)
        # Strip all HTML tags
        cleaned = re.sub(r"<[^>]+>", " ", cleaned)
        # Replace non-breaking space and newlines
        cleaned = cleaned.replace("\xa0", " ").replace("\n", " ").strip()
        # Collapse multi-spaces
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned

    def parse_tables(self, markdown_or_html: str) -> list[ParsedTable]:
        """Extract all HTML <table> elements from content and parse into ParsedTable list."""
        if not markdown_or_html or "<table>" not in markdown_or_html.lower():
            return []

        soup = BeautifulSoup(markdown_or_html, self.parser_engine)
        raw_tables = soup.find_all("table")
        parsed_list: list[ParsedTable] = []

        for table in raw_tables:
            headers: list[str] = []
            rows: list[list[str]] = []

            # Check header row (<th>)
            header_cells = table.find_all("th")
            if header_cells:
                headers = [self._clean_text(cell.get_text()) for cell in header_cells]

            # Process all <tr> rows
            tr_elements = table.find_all("tr")
            for tr in tr_elements:
                cells = tr.find_all(["td", "th"])
                if not cells:
                    continue

                row_cells = [self._clean_text(cell.get_text()) for cell in cells]

                # If we haven't found headers yet, first non-empty row becomes headers
                if not headers:
                    headers = row_cells
                    continue

                # If row cells equal headers, skip header repeat
                if row_cells == headers:
                    continue

                rows.append(row_cells)

            if headers or rows:
                parsed_list.append(ParsedTable(headers=headers, rows=rows))

        # Apply multi-page table stitching
        return self.stitch_tables(parsed_list)

    def stitch_tables(self, tables: list[ParsedTable]) -> list[ParsedTable]:
        """Merge consecutive tables that share identical column headers."""
        if not tables:
            return []

        stitched: list[ParsedTable] = []
        current: ParsedTable | None = None

        for table in tables:
            if current is None:
                current = ParsedTable(headers=list(table.headers), rows=list(table.rows), caption=table.caption)
                continue

            # Compare normalized headers
            norm_curr = [h.lower() for h in current.headers]
            norm_next = [h.lower() for h in table.headers]

            if norm_curr and norm_curr == norm_next:
                # Merge rows into current table
                current.rows.extend(table.rows)
            else:
                stitched.append(current)
                current = ParsedTable(headers=list(table.headers), rows=list(table.rows), caption=table.caption)

        if current:
            stitched.append(current)

        return stitched
