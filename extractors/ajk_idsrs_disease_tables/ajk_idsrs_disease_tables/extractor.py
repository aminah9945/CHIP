from __future__ import annotations

import re
from typing import Iterator
from chip_extractors.base import ExtractionInput, Extractor, ExtractorContext, SourceRecord
from chip_extractors.table_parser import TableParser


class AjkIdsrsDiseaseTableExtractor(Extractor):
    """Extractor for AJK IDSRS weekly surveillance disease tables."""

    name = "ajk_idsrs_disease_tables"
    extractor_version = "1.0.0"
    kafka_topic = "chip.health.ajk_idsrs.disease_case_report.v1"
    input_content_type = "text/markdown"
    input_sources = ["ajk_idsrs"]

    def __init__(self) -> None:
        self.parser = TableParser()

    def extract(
        self,
        inp: ExtractionInput,
        content: bytes,
        ctx: ExtractorContext,
    ) -> Iterator[SourceRecord]:
        """Extract overall and district-wise disease case records from AJK bulletin."""
        text = content.decode("utf-8", errors="ignore")
        tables = self.parser.parse_tables(text)

        # Parse identity for year and epi-week (e.g. ajk_idsrs:2026:W18)
        year_week = "UNKNOWN"
        parts = inp.identity.split(":")
        if len(parts) >= 3:
            year_week = f"{parts[1]}-{parts[2]}"

        for table in tables:
            headers_lower = [h.lower() for h in table.headers]
            if not headers_lower:
                continue

            # 1. Overall Cases & Deaths table (headers: Disease Name, Cases, Deaths)
            if "disease name" in headers_lower[0] and "cases" in headers_lower and "deaths" in headers_lower:
                cases_idx = headers_lower.index("cases")
                deaths_idx = headers_lower.index("deaths")

                for row in table.rows:
                    if len(row) <= max(cases_idx, deaths_idx):
                        continue
                    disease = row[0].strip()
                    if not disease:
                        continue

                    cases_val = row[cases_idx].replace(",", "").strip()
                    deaths_val = row[deaths_idx].replace(",", "").strip()

                    row_type = "total" if disease.lower() == "total" else "observation"

                    payload = {
                        "scope": "overall_ajk",
                        "district": "AJ&K",
                        "disease_raw": disease,
                        "cases_raw": cases_val,
                        "deaths_raw": deaths_val,
                        "row_type": row_type,
                        "identity": inp.identity,
                    }
                    record_key = f"AJ&K|{disease}|{year_week}"
                    yield SourceRecord(payload=payload, record_key=record_key)

            # 2. District-wise detail table (headers: Disease Name, Muzaffarabad, Jhelum Valley, ...)
            elif "disease name" in headers_lower[0] and len(headers_lower) > 3:
                districts = table.headers[1:]
                for row in table.rows:
                    if not row or len(row) < 2:
                        continue
                    disease = row[0].strip()
                    if not disease:
                        continue

                    row_type = "total" if disease.lower() == "total" else "observation"

                    for idx, district_name in enumerate(districts):
                        if idx + 1 >= len(row):
                            continue
                        val = row[idx + 1].replace(",", "").strip()

                        payload = {
                            "scope": "district_detail",
                            "district": district_name.strip(),
                            "disease_raw": disease,
                            "cases_raw": val,
                            "row_type": row_type,
                            "identity": inp.identity,
                        }
                        record_key = f"{district_name.strip()}|{disease}|{year_week}"
                        yield SourceRecord(payload=payload, record_key=record_key)
