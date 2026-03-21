from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable, Sequence

import polars as pl

from storage.catalog import read_instrument_catalog
from storage.quotes import raw_partition_path
from tools.mt5_terminal_export_sync import (
    ExportJob,
    decode_exports,
    default_common_root,
    run_terminal_export,
    write_job_manifest,
    write_startup_config,
)


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def month_partitions_between(started_at: datetime, ended_at: datetime) -> list[tuple[int, int]]:
    cursor = datetime(started_at.year, started_at.month, 1, tzinfo=UTC)
    stop = datetime(ended_at.year, ended_at.month, 1, tzinfo=UTC)
    partitions: list[tuple[int, int]] = []
    while cursor <= stop:
        partitions.append((cursor.year, cursor.month))
        if cursor.month == 12:
            cursor = datetime(cursor.year + 1, 1, 1, tzinfo=UTC)
        else:
            cursor = datetime(cursor.year, cursor.month + 1, 1, tzinfo=UTC)
    return partitions


def chunked(values: Sequence[str], size: int) -> Iterable[list[str]]:
    if size <= 0:
        raise ValueError('Chunk size must be positive')
    for index in range(0, len(values), size):
        yield list(values[index:index + size])


def symbol_partitions_exist(broker: str, symbol: str, started_at: datetime, ended_at: datetime) -> bool:
    required = month_partitions_between(started_at, ended_at)
    return all(raw_partition_path(broker=broker, symbol=symbol, year=year, month=month).exists() for year, month in required)


def resolve_symbols(
    broker: str,
    explicit_symbols: Sequence[str],
    *,
    all_symbols: bool,
    groups: Sequence[str],
    limit: int | None,
) -> list[str]:
    if explicit_symbols:
        symbols = list(dict.fromkeys(explicit_symbols))
    else:
        if not all_symbols:
            raise ValueError('Either provide --symbol or enable --all-symbols')
        catalog = read_instrument_catalog(broker)
        if groups:
            catalog = catalog.filter(pl.col('normalized_group').is_in(list(groups)))
        symbols = catalog.get_column('symbol').sort().to_list() if not catalog.is_empty() else []

    if limit is not None:
        symbols = symbols[:limit]
    return symbols


def build_jobs(symbols: Sequence[str], started_at: datetime, ended_at: datetime) -> list[ExportJob]:
    return [
        ExportJob(
            symbol=symbol,
            timeframe='M5',
            started_at=started_at,
            ended_at=ended_at,
            output_name=f"{symbol.replace('+', 'plus')}_M5_{started_at.strftime('%Y%m%d')}_{ended_at.strftime('%Y%m%d')}.bin",
        )
        for symbol in symbols
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Bulk sync MT5 history from instrument catalog using terminal-side exporter.')
    parser.add_argument('--terminal-path', required=True)
    parser.add_argument('--broker', default='bybit_mt5')
    parser.add_argument('--common-root', default=str(default_common_root()))
    parser.add_argument('--from', dest='started_at', required=True)
    parser.add_argument('--to', dest='ended_at', required=True)
    parser.add_argument('--symbol', action='append', default=[])
    parser.add_argument('--all-symbols', action='store_true')
    parser.add_argument('--group', action='append', default=[])
    parser.add_argument('--limit', type=int)
    parser.add_argument('--chunk-size', type=int, default=40)
    parser.add_argument('--skip-existing', action='store_true')
    return parser


def main() -> int:
    args = build_parser().parse_args()
    started_at = _parse_dt(args.started_at)
    ended_at = _parse_dt(args.ended_at)
    symbols = resolve_symbols(
        args.broker,
        args.symbol,
        all_symbols=args.all_symbols,
        groups=args.group,
        limit=args.limit,
    )
    if args.skip_existing:
        symbols = [symbol for symbol in symbols if not symbol_partitions_exist(args.broker, symbol, started_at, ended_at)]

    if not symbols:
        print('No symbols to export.')
        return 0

    common_root = Path(args.common_root)
    config_path = Path.cwd() / 'codex_export_run.ini'
    for chunk_index, batch in enumerate(chunked(symbols, args.chunk_size), start=1):
        jobs = build_jobs(batch, started_at, ended_at)
        write_job_manifest(common_root=common_root, jobs=jobs)
        write_startup_config(config_path=config_path, chart_symbol=batch[0])
        exit_code = run_terminal_export(terminal_path=Path(args.terminal_path), config_path=config_path)
        written = decode_exports(common_root=common_root, broker=args.broker, jobs=jobs)
        print(f'chunk={chunk_index} terminal_exit={exit_code} symbols={len(batch)} written_partitions={len(written)}')
        for symbol in batch:
            print(f'symbol={symbol}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
