from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

OPTIMIZER_COLUMNS_NO_STOP = (
    "Net",
    "Max DD",
    "Trades",
    "Lookback",
    "Omega",
    "K",
    "Score",
    "Ending",
    "PnL/DD",
    "Ulcer",
    "UPI",
    "Entry Z",
    "Exit Z",
    "Win",
    "Gross",
    "Spread",
    "Slip",
    "Comm",
    "Costs",
)

OPTIMIZER_COLUMNS_WITH_STOP = (
    "Net",
    "Max DD",
    "Trades",
    "Lookback",
    "Omega",
    "K",
    "Score",
    "Ending",
    "PnL/DD",
    "Ulcer",
    "UPI",
    "Entry Z",
    "Exit Z",
    "Stop Z",
    "Win",
    "Gross",
    "Spread",
    "Slip",
    "Comm",
    "Costs",
)

OPTIMIZER_COLUMN_SETS = (
    OPTIMIZER_COLUMNS_NO_STOP,
    OPTIMIZER_COLUMNS_WITH_STOP,
)


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _canonical_columns_for_width(width: int) -> tuple[str, ...] | None:
    for columns in OPTIMIZER_COLUMN_SETS:
        if len(columns) == width:
            return columns
    return None


def _all_known_headers(values: list[str]) -> bool:
    normalized = {_normalize_header(column) for columns in OPTIMIZER_COLUMN_SETS for column in columns}
    return all(_normalize_header(value) in normalized for value in values)


def _coerce_header_row(values: list[str]) -> tuple[str, ...] | None:
    normalized_to_canonical = {
        _normalize_header(column): column
        for columns in OPTIMIZER_COLUMN_SETS
        for column in columns
    }
    resolved = []
    for value in values:
        canonical = normalized_to_canonical.get(_normalize_header(value))
        if canonical is None:
            return None
        resolved.append(canonical)
    return tuple(resolved)


def _parse_tsv(text: str) -> tuple[tuple[str, ...], list[dict[str, str]]] | None:
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    if not lines or not any("\t" in line for line in lines):
        return None

    rows = [[cell.strip() for cell in line.split("\t")] for line in lines]
    if not rows:
        return None

    header_row = _coerce_header_row(rows[0])
    if header_row is not None:
        body = rows[1:]
        if not body:
            return header_row, []
        if not all(len(row) == len(header_row) for row in body):
            return None
        return header_row, [dict(zip(header_row, row, strict=True)) for row in body]

    width = len(rows[0])
    columns = _canonical_columns_for_width(width)
    if columns is None or not all(len(row) == width for row in rows):
        return None
    return columns, [dict(zip(columns, row, strict=True)) for row in rows]


def _parse_vertical_blocks(text: str) -> tuple[tuple[str, ...], list[dict[str, str]]] | None:
    blocks = [block.strip() for block in re.split(r"\n\s*\n+", text.strip()) if block.strip()]
    if not blocks:
        return None

    parsed_rows: list[dict[str, str]] = []
    resolved_columns: tuple[str, ...] | None = None
    for block in blocks:
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        if len(lines) % 2 != 0:
            return None
        headers = lines[0::2]
        values = lines[1::2]
        if not _all_known_headers(headers):
            return None
        columns = _coerce_header_row(headers)
        if columns is None:
            return None
        if resolved_columns is None:
            resolved_columns = columns
        elif resolved_columns != columns:
            return None
        parsed_rows.append(dict(zip(columns, values, strict=True)))

    if resolved_columns is None:
        return None
    return resolved_columns, parsed_rows


def parse_optimizer_clipboard(text: str) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    cleaned = text.strip()
    if not cleaned:
        raise ValueError("Clipboard text is empty.")

    for parser in (_parse_tsv, _parse_vertical_blocks):
        parsed = parser(cleaned)
        if parsed is not None:
            return parsed

    raise ValueError(
        "Unsupported optimizer clipboard format. Expected TSV rows or vertical header/value blocks "
        "using the visible optimizer columns."
    )


def render_markdown_table(columns: tuple[str, ...], rows: list[dict[str, str]]) -> str:
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = [
        "| " + " | ".join(str(row.get(column, "")).replace("|", "\\|") for column in columns) + " |"
        for row in rows
    ]
    return "\n".join([header, separator, *body]) if body else "\n".join([header, separator])


def _read_text(path: str | None) -> str:
    if path:
        return Path(path).read_text(encoding="utf-8")
    return sys.stdin.read()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Convert optimizer table clipboard text into a markdown table.")
    parser.add_argument("--input", type=str, help="Path to a text file with clipboard contents. Defaults to stdin.")
    args = parser.parse_args(argv)

    try:
        columns, rows = parse_optimizer_clipboard(_read_text(args.input))
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(render_markdown_table(columns, rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
