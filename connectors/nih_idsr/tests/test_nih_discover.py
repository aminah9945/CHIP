from pathlib import Path
from unittest.mock import MagicMock
from nih_idsr.connector import NihIdsrConnector


def test_nih_derive_identity_patterns():
    conn = NihIdsrConnector()
    assert conn.derive_identity("Week-01-2025.md") == "idsr:2025:W01"
    assert conn.derive_identity("Weekly Report-01-2024.md") == "idsr:2024:W01"
    assert conn.derive_identity("IDSR-Weekly-Report-11-2022.md") == "idsr:2022:W11"
    assert conn.derive_identity("IDSRS Weekly Report-01-2026-NEW.md") == "idsr:2026:W01"
    assert conn.derive_identity("IDSRS Weekly Report-02-2026-updated (1).md") == "idsr:2026:W02"
    assert conn.derive_identity("Weekly_Report_22_2023.md") == "idsr:2023:W22"
    assert conn.derive_identity("IDSR Week 13 Bulletin (2025).md") == "idsr:2025:W13"


def test_nih_derive_identity_yearless():
    conn = NihIdsrConnector(source_dir="Data_sources_1/NIH/MD")
    # File IDSR-Weekly-Report-50.md exists on disk
    identity = conn.derive_identity("Data_sources_1/NIH/MD/IDSR-Weekly-Report-50.md")
    assert identity.startswith("idsr:")
    assert identity.endswith(":W50")


def test_nih_discover_real_files():
    conn = NihIdsrConnector(source_dir="Data_sources_1/NIH/MD")
    ctx = MagicMock()
    items = list(conn.discover(ctx))

    assert len(items) == 174
    identities = {item.identity for item in items}
    assert "idsr:2025:W01" in identities
    assert "idsr:2026:W01" in identities
    assert "idsr:2022:W11" in identities
