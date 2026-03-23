from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

import polars as pl

from domain.data.co_movers import CO_MOVER_GROUPS as PAIR_CLUSTERS
from domain.data.co_movers import CoMoverGroup as PairCluster
from storage.catalog import read_instrument_catalog


CURRENCY_NAMES = {
    "AUD": "Australian Dollar",
    "BRL": "Brazilian Real",
    "CAD": "Canadian Dollar",
    "CHF": "Swiss Franc",
    "CNH": "Chinese Yuan",
    "EUR": "Euro",
    "GBP": "British Pound",
    "HKD": "Hong Kong Dollar",
    "IDR": "Indonesian Rupiah",
    "INR": "Indian Rupee",
    "JPY": "Japanese Yen",
    "MXN": "Mexican Peso",
    "NOK": "Norwegian Krone",
    "NZD": "New Zealand Dollar",
    "SEK": "Swedish Krona",
    "SGD": "Singapore Dollar",
    "THB": "Thai Baht",
    "TRY": "Turkish Lira",
    "TWD": "Taiwan Dollar",
    "USD": "US Dollar",
    "ZAR": "South African Rand",
}

METAL_NAMES = {
    "XAU": "Gold",
    "XAG": "Silver",
    "XPT": "Platinum",
    "XPD": "Palladium",
}

SPECIAL_MEANINGS = {
    "COPPER-C": "Copper cash contract",
    "Coffee-C": "Coffee cash contract",
    "Cocoa-C": "Cocoa cash contract",
    "Cotton-C": "Cotton cash contract",
    "Sugar-C": "Sugar cash contract",
    "Wheat-C": "Wheat cash contract",
    "GAS-C": "Gasoline cash contract",
    "GASOIL-C": "Low sulphur gasoil cash contract",
    "NG-C": "Natural gas cash contract",
    "UKOUSD": "Brent crude oil vs US Dollar",
    "USOUSD": "WTI crude oil vs US Dollar",
}

FAMILY_ORDER = [
    "forex",
    "indices",
    "energy",
    "metals",
    "softs_agri",
    "stocks",
    "commodities_other",
    "other",
]

FAMILY_TITLES = {
    "forex": "FX",
    "indices": "Indices",
    "energy": "Energy",
    "metals": "Metals",
    "softs_agri": "Softs / Agriculture",
    "stocks": "US Stocks",
    "commodities_other": "Other Commodities",
    "other": "Other / Review Manually",
}

BROAD_GROUP_BY_FAMILY = {
    "forex": "forex",
    "indices": "indices",
    "energy": "commodities",
    "metals": "commodities",
    "softs_agri": "commodities",
    "stocks": "stocks",
    "commodities_other": "commodities",
    "other": "other",
}



def _clean_text(value: object) -> str:
    text = str(value or "").replace("\xa0", " ").strip()
    return " ".join(text.split())


def _symbol_no_suffix(symbol: str) -> str:
    return symbol[:-1] if symbol.endswith("+") else symbol


def infer_meaning(symbol: str, description: str) -> str:
    clean = _symbol_no_suffix(symbol)
    if description:
        return description
    if clean in SPECIAL_MEANINGS:
        return SPECIAL_MEANINGS[clean]
    if len(clean) == 6 and clean.isalpha():
        base = clean[:3]
        quote = clean[3:]
        if base in CURRENCY_NAMES and quote in CURRENCY_NAMES:
            return f"{CURRENCY_NAMES[base]} vs {CURRENCY_NAMES[quote]}"
        if base in METAL_NAMES and quote in CURRENCY_NAMES:
            return f"{METAL_NAMES[base]} vs {CURRENCY_NAMES[quote]}"
    return ""


def human_family(symbol: str, path: str, description: str) -> str:
    if path.startswith("Stocks\\"):
        return "stocks"
    if path.startswith("CFDs\\Indices") or path.startswith("Nikkei"):
        return "indices"
    if path.startswith("Oil\\") or symbol in {"NG-C", "GAS-C", "GASOIL-C"}:
        return "energy"
    if symbol in {"COPPER-C", "XAGUSD", "XAUAUD+", "XAUEUR+", "XAUJPY+", "XAUUSD+", "XPDUSD", "XPTUSD"}:
        return "metals"
    if symbol.startswith(("XAU", "XAG", "XPT", "XPD")) or path.startswith(("Gold+\\", "Silver\\")):
        return "metals"
    if symbol in {"Coffee-C", "Cocoa-C", "Cotton-C", "Corn-C", "Wheat-C", "Soybn-C", "Sugar-C"}:
        return "softs_agri"
    if path.startswith("Forex+\\") or path.startswith("Forex Major\\"):
        return "forex"
    if path.startswith("Commodities\\"):
        return "commodities_other"
    return "other"


def inventory_frame(broker: str) -> pl.DataFrame:
    raw = read_instrument_catalog(broker)
    rows: list[dict[str, object]] = []
    for row in raw.to_dicts():
        symbol = _clean_text(row.get("symbol"))
        path = _clean_text(row.get("path"))
        description = _clean_text(row.get("description"))
        family = human_family(symbol, path, description)
        rows.append(
            {
                "symbol": symbol,
                "meaning": infer_meaning(symbol, description),
                "description": description,
                "human_family": family,
                "human_family_title": FAMILY_TITLES.get(family, family),
                "catalog_group": _clean_text(row.get("normalized_group")),
                "path": path,
            }
        )
    return pl.DataFrame(rows).sort(["human_family_title", "symbol"])


def catalog_anomalies(frame: pl.DataFrame) -> pl.DataFrame:
    broad = frame.with_columns(
        pl.col("human_family").replace(BROAD_GROUP_BY_FAMILY).alias("human_broad_group")
    )
    return (
        broad.filter(pl.col("catalog_group") != pl.col("human_broad_group"))
        .select(["symbol", "catalog_group", "human_family_title", "path"])
        .sort("symbol")
    )


def available_clusters(symbols: set[str]) -> list[PairCluster]:
    result: list[PairCluster] = []
    for cluster in PAIR_CLUSTERS:
        if all(symbol in symbols for symbol in cluster.symbols):
            result.append(cluster)
    return result


def _render_cluster_table(clusters: Iterable[PairCluster]) -> str:
    lines = [
        "| Cluster | Symbols | Why they usually move together | Notes |",
        "| --- | --- | --- | --- |",
    ]
    for cluster in clusters:
        lines.append(
            f"| {cluster.name} | `{', '.join(cluster.symbols)}` | {cluster.why} | {cluster.notes} |"
        )
    return "\n".join(lines)


def _render_inventory_section(frame: pl.DataFrame, family: str) -> str:
    family_rows = frame.filter(pl.col("human_family") == family).select(
        ["symbol", "meaning", "description", "catalog_group", "path"]
    )
    title = FAMILY_TITLES[family]
    lines = [f"## {title} ({family_rows.height})", "", "| Symbol | Meaning / what it is | Catalog description | Catalog group | Raw path |", "| --- | --- | --- | --- | --- |"]
    for row in family_rows.to_dicts():
        lines.append(
            f"| `{row['symbol']}` | {row['meaning'] or '-'} | {row['description'] or '-'} | `{row['catalog_group'] or '-'}` | `{row['path'] or '-'}` |"
        )
    return "\n".join(lines)


def render_markdown(broker: str, frame: pl.DataFrame) -> str:
    generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    total = frame.height
    counts = frame.group_by("human_family_title").len().sort("human_family_title")
    anomalies = catalog_anomalies(frame)
    symbols = set(frame.get_column("symbol").to_list())
    clusters = available_clusters(symbols)
    cluster_sections = sorted({cluster.section for cluster in clusters})

    lines = [
        "# Bybit MT5 instrument map and co-move pairs",
        "",
        f"Generated from local catalog `data/catalog/{broker}/instrument_catalog.parquet` on {generated_at}.",
        "",
        "## What this file is",
        "",
        f"- Local inventory only: `{total}` instruments currently present in this workspace.",
        "- This file separates the raw catalog group from a path-based human family, because the raw catalog contains some classification mistakes.",
        "- `+` at the end of a symbol is a broker suffix, not a different economic asset.",
        "- `-C` in this catalog usually means a cash commodity contract.",
        "",
        "## Family counts",
        "",
    ]
    for row in counts.to_dicts():
        lines.append(f"- {row['human_family_title']}: `{int(row['len'])}`")

    lines.extend(
        [
            "",
            "## Catalog quality notes",
            "",
            f"- Raw `normalized_group` disagrees with path-based family for `{anomalies.height}` symbols.",
            "- This matters for pair discovery: some FX, metals and stocks are locally tagged into the wrong group.",
            "",
            "| Symbol | Raw catalog group | Human family | Raw path |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in anomalies.head(24).to_dicts():
        lines.append(
            f"| `{row['symbol']}` | `{row['catalog_group']}` | {row['human_family_title']} | `{row['path']}` |"
        )

    lines.extend(
        [
            "",
            "## Co-move clusters and practical pair ideas",
            "",
            "These are not guaranteed stationary pairs. They are the first places worth scanning because they share the same macro driver, sector beta or quote-currency regime.",
            "",
        ]
    )
    for section in cluster_sections:
        lines.extend([f"### {section}", "", _render_cluster_table(cluster for cluster in clusters if cluster.section == section), ""])

    lines.extend(
        [
            "## Full local inventory",
            "",
            "Use the TSV file for filtering and sorting in an editor or spreadsheet:",
            "",
            f"- `docs/notes/{broker}_instrument_inventory.tsv`",
            "",
        ]
    )
    for family in FAMILY_ORDER:
        if frame.filter(pl.col("human_family") == family).is_empty():
            continue
        lines.extend([_render_inventory_section(frame, family), ""])

    return "\n".join(lines).strip() + "\n"


def write_outputs(broker: str, markdown_path: Path, tsv_path: Path) -> tuple[Path, Path]:
    frame = inventory_frame(broker)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    tsv_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(broker, frame), encoding="utf-8")
    frame.write_csv(tsv_path, separator="\t")
    return markdown_path, tsv_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Export local instrument inventory and co-move pair reference.")
    parser.add_argument("--broker", default="bybit_mt5")
    parser.add_argument("--markdown-out", default="docs/notes/bybit_mt5_instrument_map.md")
    parser.add_argument("--tsv-out", default="docs/notes/bybit_mt5_instrument_inventory.tsv")
    args = parser.parse_args()

    markdown_path, tsv_path = write_outputs(
        broker=str(args.broker),
        markdown_path=Path(str(args.markdown_out)),
        tsv_path=Path(str(args.tsv_out)),
    )
    print(f"markdown={markdown_path}")
    print(f"tsv={tsv_path}")


if __name__ == "__main__":
    main()
