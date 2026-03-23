from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, date, datetime
from math import isfinite
from pathlib import Path
import re
from zipfile import ZIP_DEFLATED, ZipFile
from xml.sax.saxutils import escape

from bokeh.models import DataTable

EXPORT_ROOT = Path(__file__).resolve().parents[2] / "docs" / "tables"


def sanitize_filename_part(value: object, *, fallback: str = "na") -> str:
    text = str(value or "").strip()
    text = re.sub(r"[^\w.-]+", "_", text, flags=re.ASCII)
    text = re.sub(r"_+", "_", text).strip("._")
    return text or fallback


def build_table_export_path(
    *,
    block_name: str,
    symbol_1: object,
    symbol_2: object,
    timeframe: object,
    exported_at: datetime | None = None,
    root: Path = EXPORT_ROOT,
) -> Path:
    moment = (exported_at or datetime.now(UTC)).astimezone(UTC)
    stamp = moment.strftime("%Y%m%dT%H%M%SZ")
    filename = "__".join(
        [
            sanitize_filename_part(block_name, fallback="table"),
            sanitize_filename_part(symbol_1),
            sanitize_filename_part(symbol_2),
            sanitize_filename_part(timeframe),
            stamp,
        ]
    )
    return root / f"{filename}.xlsx"


def _column_letter(index: int) -> str:
    letters: list[str] = []
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        letters.append(chr(ord("A") + remainder))
    return "".join(reversed(letters))


def _stringify_cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "item") and callable(getattr(value, "item")):
        return _stringify_cell(value.item())
    return str(value)


def _cell_xml(ref: str, value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return f'<c r="{ref}" t="b"><v>{1 if value else 0}</v></c>'
    if hasattr(value, "item") and callable(getattr(value, "item")):
        value = value.item()
    if isinstance(value, (int, float)) and isfinite(float(value)):
        return f'<c r="{ref}"><v>{value}</v></c>'
    text = _stringify_cell(value)
    if text == "":
        return ""
    preserve = ' xml:space="preserve"' if text != text.strip() or "\n" in text else ""
    return f'<c r="{ref}" t="inlineStr"><is><t{preserve}>{escape(text)}</t></is></c>'


def _sheet_xml(rows: Sequence[Sequence[object]]) -> str:
    max_columns = max((len(row) for row in rows), default=1)
    max_rows = max(len(rows), 1)
    dimension = f"A1:{_column_letter(max_columns)}{max_rows}"
    xml_rows: list[str] = []
    for row_index, row in enumerate(rows, start=1):
        cells = [
            _cell_xml(f"{_column_letter(column_index)}{row_index}", value)
            for column_index, value in enumerate(row, start=1)
        ]
        xml_rows.append(f'<row r="{row_index}">{"".join(cell for cell in cells if cell)}</row>')
    sheet_data = "".join(xml_rows) or '<row r="1"></row>'
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="{dimension}"/>'
        '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        f"<sheetData>{sheet_data}</sheetData>"
        "</worksheet>"
    )


def _rels_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>'
        '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>'
        "</Relationships>"
    )


def _workbook_xml(sheet_name: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<sheets><sheet name="{escape(sheet_name)}" sheetId="1" r:id="rId1"/></sheets>'
        "</workbook>"
    )


def _workbook_rels_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        "</Relationships>"
    )


def _content_types_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
        '<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>'
        "</Types>"
    )


def _core_properties_xml(created_at: datetime) -> str:
    stamp = created_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:dcterms="http://purl.org/dc/terms/" '
        'xmlns:dcmitype="http://purl.org/dc/dcmitype/" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        "<dc:creator>MT Pair Trading Service</dc:creator>"
        "<cp:lastModifiedBy>MT Pair Trading Service</cp:lastModifiedBy>"
        f'<dcterms:created xsi:type="dcterms:W3CDTF">{stamp}</dcterms:created>'
        f'<dcterms:modified xsi:type="dcterms:W3CDTF">{stamp}</dcterms:modified>'
        "</cp:coreProperties>"
    )


def _app_properties_xml(sheet_name: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" '
        'xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
        '<Application>MT Pair Trading Service</Application>'
        '<HeadingPairs><vt:vector size="2" baseType="variant">'
        '<vt:variant><vt:lpstr>Worksheets</vt:lpstr></vt:variant>'
        '<vt:variant><vt:i4>1</vt:i4></vt:variant>'
        "</vt:vector></HeadingPairs>"
        '<TitlesOfParts><vt:vector size="1" baseType="lpstr">'
        f"<vt:lpstr>{escape(sheet_name)}</vt:lpstr>"
        "</vt:vector></TitlesOfParts>"
        "</Properties>"
    )


def _sheet_name(block_name: str) -> str:
    cleaned = re.sub(r"[:\\\\/*?\\[\\]]+", " ", str(block_name)).strip() or "Table Export"
    return cleaned[:31]


def data_table_to_rows(table: DataTable) -> tuple[list[str], list[list[object]]]:
    headers: list[str] = []
    fields: list[str] = []
    for column in table.columns:
        field = getattr(column, "field", None)
        if not field:
            continue
        headers.append(str(column.title or field))
        fields.append(str(field))

    data = table.source.data
    row_count = max((len(data.get(field, [])) for field in fields), default=0)
    rows: list[list[object]] = []
    for row_index in range(row_count):
        row: list[object] = []
        for field in fields:
            values = data.get(field, [])
            row.append(values[row_index] if row_index < len(values) else None)
        rows.append(row)
    return headers, rows


def export_table_to_xlsx(
    *,
    table: DataTable,
    block_name: str,
    symbol_1: object,
    symbol_2: object,
    timeframe: object,
    metadata_rows: Sequence[tuple[str, object]],
    root: Path = EXPORT_ROOT,
) -> Path:
    headers, table_rows = data_table_to_rows(table)
    if not headers:
        raise ValueError("Table has no visible columns to export.")
    if not table_rows:
        raise ValueError("Table has no rows to export.")

    exported_at = datetime.now(UTC)
    output_path = build_table_export_path(
        block_name=block_name,
        symbol_1=symbol_1,
        symbol_2=symbol_2,
        timeframe=timeframe,
        exported_at=exported_at,
        root=root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    workbook_rows: list[list[object]] = [
        ["Parameter", "Value"],
        *[[key, value] for key, value in metadata_rows],
        [],
        headers,
        *table_rows,
    ]
    sheet_name = _sheet_name(block_name)

    with ZipFile(output_path, mode="w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", _content_types_xml())
        archive.writestr("_rels/.rels", _rels_xml())
        archive.writestr("xl/workbook.xml", _workbook_xml(sheet_name))
        archive.writestr("xl/_rels/workbook.xml.rels", _workbook_rels_xml())
        archive.writestr("xl/worksheets/sheet1.xml", _sheet_xml(workbook_rows))
        archive.writestr("docProps/core.xml", _core_properties_xml(exported_at))
        archive.writestr("docProps/app.xml", _app_properties_xml(sheet_name))
    return output_path


def metadata_rows_from_mapping(items: Iterable[tuple[str, object]]) -> list[tuple[str, object]]:
    rows: list[tuple[str, object]] = []
    for key, value in items:
        if isinstance(value, dict):
            value = ", ".join(f"{sub_key}={sub_value}" for sub_key, sub_value in value.items())
        elif isinstance(value, (list, tuple)):
            value = ", ".join(str(item) for item in value)
        rows.append((str(key), _stringify_cell(value)))
    return rows
