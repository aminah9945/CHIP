from chip_extractors.table_parser import TableParser


def test_parse_single_table():
    html_content = """
    # Section Title
    <table>
      <thead>
        <tr><th>District</th><th>ILI</th><th>Malaria</th></tr>
      </thead>
      <tbody>
        <tr><td>D.G KHAN</td><td>150</td><td>25</td></tr>
        <tr><td>R.Y. KHAN</td><td>300</td><td>10</td></tr>
      </tbody>
    </table>
    """
    parser = TableParser()
    tables = parser.parse_tables(html_content)

    assert len(tables) == 1
    assert tables[0].headers == ["District", "ILI", "Malaria"]
    assert len(tables[0].rows) == 2
    assert tables[0].rows[0] == ["D.G KHAN", "150", "25"]


def test_stitch_multi_page_split_table():
    html_content = """
    <!-- Page 1 -->
    <table>
      <tr><th>District</th><th>ILI</th></tr>
      <tr><td>District A</td><td>100</td></tr>
    </table>

    <p>Page 2 continued...</p>

    <!-- Page 2 split table -->
    <table>
      <tr><th>District</th><th>ILI</th></tr>
      <tr><td>District B</td><td>200</td></tr>
    </table>
    """
    parser = TableParser()
    tables = parser.parse_tables(html_content)

    # Multi-page stitching merges into 1 table
    assert len(tables) == 1
    assert tables[0].headers == ["District", "ILI"]
    assert len(tables[0].rows) == 2
    assert tables[0].rows[0] == ["District A", "100"]
    assert tables[0].rows[1] == ["District B", "200"]
