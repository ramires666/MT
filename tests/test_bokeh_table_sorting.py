from __future__ import annotations

from pathlib import Path


def test_all_main_bokeh_tables_enable_sorting() -> None:
    main_py = Path("src/bokeh_app/main.py").read_text(encoding="utf-8")

    assert "sortable=False" not in main_py
    assert main_py.count("DataTable(") == main_py.count("sortable=True")
