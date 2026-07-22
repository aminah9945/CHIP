from __future__ import annotations

from typing import Iterator
from chip_extractors.base import ExtractionInput, Extractor, ExtractorContext, SourceRecord
from chip_extractors.table_parser import TableParser


class PitbDssDiseaseTableExtractor(Extractor):
    """Extractor for PITB-DSS / HRS communicable disease tables."""

    name = "pitb_dss_disease_tables"
    extractor_version = "1.0.0"
    kafka_topic = "chip.health.pitb_dss.disease_case_report.v1"
    input_content_type = "text/markdown"
    input_sources = ["pitb_dss"]

    def __init__(self) -> None:
        self.parser = TableParser()

    def extract(
        self,
        inp: ExtractionInput,
        content: bytes,
        ctx: ExtractorContext,
    ) -> Iterator[SourceRecord]:
        """Extract disease case counts from PITB-DSS bulletin tables."""
        text = content.decode("utf-8", errors="ignore")
        tables = self.parser.parse_tables(text)

        year_week = "UNKNOWN"
        parts = inp.identity.split(":")
        if len(parts) >= 3:
            year_week = f"{parts[1]}-{parts[2]}"

        for table in tables:
            headers_lower = [h.lower() for h in table.headers]
            if not headers_lower:
                continue

            # Case 1: Standard communicable disease table (Disease, Number of cases)
            if "disease" in headers_lower[0] and len(headers_lower) == 2 and "case" in headers_lower[1]:
                for row in table.rows:
                    if len(row) < 2:
                        continue
                    disease = row[0].strip()
                    if not disease or disease.lower() == "disease":
                        continue

                    cases_val = row[1].replace(",", "").strip()
                    row_type = "total" if disease.lower() == "total" else "observation"

                    payload = {
                        "scope": "punjab_province",
                        "district": "Punjab",
                        "disease_raw": disease,
                        "cases_raw": cases_val,
                        "row_type": row_type,
                        "identity": inp.identity,
                    }
                    record_key = f"Punjab|{disease}|{year_week}"
                    yield SourceRecord(payload=payload, record_key=record_key)

            # Case 2: Transposed matrix (Disease, District 1, District 2, ...)
            elif "disease" in headers_lower[0] and len(headers_lower) > 2:
                districts = table.headers[1:]
                for row in table.rows:
                    if len(row) < 2:
                        continue
                    disease = row[0].strip()
                    if not disease:
                        continue

                    row_type = "total" if disease.lower() == "total" else "observation"

                    for idx, district in enumerate(districts):
                        if idx + 1 >= len(row):
                            continue
                        cases_val = row[idx + 1].replace(",", "").strip()

                        payload = {
                            "scope": "district_detail",
                            "district": district.strip(),
                            "disease_raw": disease,
                            "cases_raw": cases_val,
                            "row_type": row_type,
                            "identity": inp.identity,
                        }
                        record_key = f"{district.strip()}|{disease}|{year_week}"
                        yield SourceRecord(payload=payload, record_key=record_key)
