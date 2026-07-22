from __future__ import annotations

from typing import Iterator
from chip_extractors.base import ExtractionInput, Extractor, ExtractorContext, SourceRecord
from chip_extractors.table_parser import TableParser


class NihIdsrDiseaseTableExtractor(Extractor):
    """Extractor for NIH IDSR weekly bulletin disease tables."""

    name = "nih_idsr_disease_tables"
    extractor_version = "1.0.0"
    kafka_topic = "chip.health.nih_idsr.disease_case_report.v1"
    input_content_type = "text/markdown"
    input_sources = ["nih_idsr"]

    PROVINCES = {"ajk", "balochistan", "gb", "ict", "kp", "punjab", "sindh", "national", "total"}

    def __init__(self) -> None:
        self.parser = TableParser()

    def extract(
        self,
        inp: ExtractionInput,
        content: bytes,
        ctx: ExtractorContext,
    ) -> Iterator[SourceRecord]:
        """Extract disease case counts from NIH IDSR tables (Province & District tables)."""
        text = content.decode("utf-8", errors="ignore")
        tables = self.parser.parse_tables(text)

        year_week = "UNKNOWN"
        parts = inp.identity.split(":")
        if len(parts) >= 3:
            year_week = f"{parts[1]}-{parts[2]}"

        for table in tables:
            headers_lower = [h.lower().strip() for h in table.headers]
            if not headers_lower or len(headers_lower) < 2:
                continue

            # Case A: Table 1 - Province/Area wise distribution (Diseases, AJK, Balochistan, GB, ICT, KP, Punjab, Sindh, Total)
            if "disease" in headers_lower[0] and any(p in headers_lower for p in ["ajk", "balochistan", "kp", "sindh", "punjab"]):
                provinces = table.headers[1:]
                for row in table.rows:
                    if len(row) < 2:
                        continue
                    disease = row[0].strip()
                    if not disease or disease.lower() in ("diseases", "disease"):
                        continue

                    row_type = "total" if disease.lower() == "total" else "observation"

                    for idx, province in enumerate(provinces):
                        if idx + 1 >= len(row):
                            continue
                        cases_val = row[idx + 1].replace(",", "").strip()

                        payload = {
                            "scope": "province_summary",
                            "province": province.strip(),
                            "disease_raw": disease,
                            "cases_raw": cases_val,
                            "row_type": row_type,
                            "identity": inp.identity,
                        }
                        record_key = f"{province.strip()}|{disease}|{year_week}"
                        yield SourceRecord(payload=payload, record_key=record_key)

            # Case B: District level tables (Diseases as row / Districts as cols OR District as rows / Diseases as cols)
            elif ("disease" in headers_lower[0] or "district" in headers_lower[0]) and len(headers_lower) > 3:
                cols = table.headers[1:]
                is_disease_row = "disease" in headers_lower[0]

                for row in table.rows:
                    if len(row) < 2:
                        continue
                    first_col = row[0].strip()
                    if not first_col or first_col.lower() in ("diseases", "district", "disease"):
                        continue

                    row_type = "total" if first_col.lower() == "total" else "observation"

                    for idx, col_header in enumerate(cols):
                        if idx + 1 >= len(row):
                            continue
                        cases_val = row[idx + 1].replace(",", "").strip()

                        if is_disease_row:
                            district = col_header.strip()
                            disease = first_col
                        else:
                            district = first_col
                            disease = col_header.strip()

                        payload = {
                            "scope": "district_detail",
                            "district": district,
                            "disease_raw": disease,
                            "cases_raw": cases_val,
                            "row_type": row_type,
                            "identity": inp.identity,
                        }
                        record_key = f"{district}|{disease}|{year_week}"
                        yield SourceRecord(payload=payload, record_key=record_key)
