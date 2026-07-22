from pathlib import Path
from unittest.mock import MagicMock
from pitb_dss.connector import PitbDssConnector


def test_pitb_dss_derive_identity():
    conn = PitbDssConnector()
    assert conn.derive_identity("Data_sources_1/PITB-DSS/2015/MD/DSS-Bulletin-Week-11.md") == "pitb_dss:2015:W11"
    assert conn.derive_identity("DSS Bulletin Week 12-2016.md") == "pitb_dss:2016:W12"
    assert conn.derive_identity("DSS Bulletin Week-2-2016.md") == "pitb_dss:2016:W02"
    assert conn.derive_identity("HRS Bulletin Week 20_2016.md") == "pitb_dss:2016:W20"
    assert conn.derive_identity("HRS Bulletin Week 8- 2016.md") == "pitb_dss:2016:W08"
    assert conn.derive_identity("HRS-Bulletin-Week-44-2016.md") == "pitb_dss:2016:W44"
    assert conn.derive_identity("HRS Bulletin Week 5,2017.md") == "pitb_dss:2017:W05"
    assert conn.derive_identity("HRS Bulletin Week 10-2018.md") == "pitb_dss:2018:W10"


def test_pitb_dss_discover_real_files():
    conn = PitbDssConnector(source_dir="Data_sources_1/PITB-DSS")
    ctx = MagicMock()
    items = list(conn.discover(ctx))

    # 169 files total - 1 junk file (_.md) = 168 valid items
    assert len(items) == 168
    identities = {item.identity for item in items}
    assert "pitb_dss:2015:W11" in identities
    assert "pitb_dss:2016:W02" in identities
    assert "pitb_dss:2017:W05" in identities
