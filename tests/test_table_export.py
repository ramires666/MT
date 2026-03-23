from __future__ import annotations

from datetime import UTC, datetime
from zipfile import ZipFile

from bokeh.models import ColumnDataSource, DataTable, TableColumn

from bokeh_app.table_export import build_table_export_path, export_table_to_xlsx


def test_build_table_export_path_uses_block_pair_timeframe(tmp_path) -> None:
    path = build_table_export_path(
        block_name="Optimization Results",
        symbol_1="AUDUSD+",
        symbol_2="CADCHF+",
        timeframe="M15",
        exported_at=datetime(2026, 3, 23, 12, 34, 56, tzinfo=UTC),
        root=tmp_path,
    )

    assert path.parent == tmp_path
    assert path.name == "Optimization_Results__AUDUSD__CADCHF__M15__20260323T123456Z.xlsx"


def test_export_table_to_xlsx_writes_metadata_and_table_rows(tmp_path) -> None:
    table = DataTable(
        source=ColumnDataSource(
            {
                "net_profit": [248.62],
                "max_drawdown": [-366.22],
                "entry_z": [2.0],
            }
        ),
        columns=[
            TableColumn(field="net_profit", title="Net"),
            TableColumn(field="max_drawdown", title="Max DD"),
            TableColumn(field="entry_z", title="Entry Z"),
        ],
    )

    output = export_table_to_xlsx(
        table=table,
        block_name="Optimization Results",
        symbol_1="AUDUSD+",
        symbol_2="CADCHF+",
        timeframe="M15",
        metadata_rows=[
            ("Symbol 1", "AUDUSD+"),
            ("Symbol 2", "CADCHF+"),
            ("Timeframe", "M15"),
            ("Optimization Period", "2026-01-01T00:00:00Z .. 2026-02-01T00:00:00Z"),
        ],
        root=tmp_path,
    )

    assert output.exists()
    with ZipFile(output) as archive:
        names = set(archive.namelist())
        assert "[Content_Types].xml" in names
        assert "xl/worksheets/sheet1.xml" in names
        sheet_xml = archive.read("xl/worksheets/sheet1.xml").decode("utf-8")
        assert "Parameter" in sheet_xml
        assert "Optimization Period" in sheet_xml
        assert "Net" in sheet_xml
        assert "Max DD" in sheet_xml
        assert "Entry Z" in sheet_xml
        assert "248.62" in sheet_xml
        assert "-366.22" in sheet_xml
