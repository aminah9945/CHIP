from pathlib import Path
from unittest.mock import MagicMock
from dhis_punjab_weekly.connector import DhisPunjabWeeklyConnector


def test_dhis_derive_identity():
    conn = DhisPunjabWeeklyConnector()
    assert conn.derive_identity("week_18.md", year_hint=2022) == "dhis_punjab_weekly:2022:W18"
    assert conn.derive_identity("Week_18(1).md", year_hint=2022) == "dhis_punjab_weekly:2022:W18"
    assert conn.derive_identity("Week_1.md", year_hint=2022) == "dhis_punjab_weekly:2022:W01"


def test_dhis_discover_real_files():
    conn = DhisPunjabWeeklyConnector(source_dir="Data_sources_1/DHIS/MD")
    ctx = MagicMock()
    items = list(conn.discover(ctx))

    # All discovered items must be from year 2022
    assert len(items) > 0
    for item in items:
        assert item.identity.startswith("dhis_punjab_weekly:2022:W")


def test_dhis_non_weekly_silent_skip():
    conn = DhisPunjabWeeklyConnector(source_dir="Data_sources_1/DHIS/MD")
    ctx = MagicMock()
    items = list(conn.discover(ctx))
    source_uris = [item.source_uri for item in items]

    # Non-weekly files should NOT be in discovered items
    for uri in source_uris:
        assert "Annual_Report" not in uri
        assert "Analysis_2021" not in uri
        assert "lahore_measles" not in uri
