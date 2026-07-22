from __future__ import annotations

from typing import Iterator
from chip_extractors.base import ExtractionInput, Extractor, ExtractorContext, SourceRecord
from chip_extractors.table_parser import TableParser


class DhisPunjabDiseaseTableExtractor(Extractor):
    """Extractor for DHIS Punjab weekly prone epidemic & OPD disease tables."""

    name = "dhis_punjab_disease_tables"
    extractor_version = "1.0.0"
    kafka_topic = "chip.health.dhis_punjab_weekly.disease_case_report.v1"
    input_content_type = "text/markdown"
    input_sources = ["dhis_punjab_weekly"]

    def __init__(self) -> None:
        self.parser = TableParser()

    def extract(
        self,
        inp: ExtractionInput,
        content: bytes,
        ctx: ExtractorContext,
    ) -> Iterator[SourceRecord]:
        """Extract disease case records from DHIS feedback report tables."""
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

            # Check for Table 2: Prone Epidemic Diseases (DISTRICT, Seasonal Influenza, AFP, Susp Measles, etc)
            if "district" in headers_lower[0] and any(
                term in " ".join(headers_lower) for term in ["influenza", "afp", "measles", "dengue", "typhoid"]
            ):
                diseases = table.headers[1:]
                for row in table.rows:
                    if len(row) < 2:
                        continue
                    district = row[0].strip()
                    if not district or district.lower() == "district":
                        continue

                    row_type = "total" if district.lower() == "total" else "observation"

                    for idx, disease_name in enumerate(diseases):
                        if idx + 1 >= len(row):
                            continue
                        cases_val = row[idx + 1].replace(",", "").strip()
                        if not cases_val:
                            cases_val = "0"

                        payload = {
                            "scope": "district_detail",
                            "district": district,
                            "disease_raw": disease_name.strip(),
                            "cases_raw": cases_val,
                            "row_type": row_type,
                            "identity": inp.identity,
                        }
                        record_key = f"{district}|{disease_name.strip()}|{year_week}"
                        yield SourceRecord(payload=payload, record_key=record_key)
