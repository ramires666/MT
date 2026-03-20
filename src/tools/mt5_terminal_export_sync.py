from __future__ import annotations

import argparse
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from storage.quotes import write_m5_quotes
from tools.mt5_binary_export import read_codex_binary


@dataclass(slots=True)
class ExportJob:
    symbol: str
    timeframe: str
    started_at: datetime
    ended_at: datetime
    output_name: str


def default_common_root() -> Path:
    return Path.home() / 'AppData' / 'Roaming' / 'MetaQuotes' / 'Terminal' / 'Common' / 'Files'


def write_job_manifest(common_root: Path, jobs: list[ExportJob]) -> Path:
    codex_dir = common_root / 'Codex'
    codex_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = codex_dir / 'history_job.txt'
    lines = [
        f"{job.symbol}|{job.timeframe}|{job.started_at.strftime('%Y.%m.%d %H:%M:%S')}|{job.ended_at.strftime('%Y.%m.%d %H:%M:%S')}|Codex\\exports\\{job.output_name}"
        for job in jobs
    ]
    manifest_path.write_text("\n".join(lines) + "\n", encoding='utf-16')
    return manifest_path


def write_startup_config(config_path: Path, chart_symbol: str, script_name: str = 'CodexHistoryExport') -> Path:
    config_path.write_text(
        "[StartUp]\r\n"
        f"Script={script_name}\r\n"
        f"Symbol={chart_symbol}\r\n"
        "Period=M5\r\n"
        "ShutdownTerminal=1\r\n",
        encoding='utf-8',
    )
    return config_path


def run_terminal_export(terminal_path: Path, config_path: Path) -> int:
    completed = subprocess.run([str(terminal_path), f'/config:{config_path}'], check=False)
    return completed.returncode


def decode_exports(common_root: Path, broker: str, jobs: list[ExportJob]) -> list[Path]:
    written: list[Path] = []
    export_root = common_root / 'Codex' / 'exports'
    for job in jobs:
        frame = read_codex_binary(export_root / job.output_name)
        written.extend(write_m5_quotes(frame=frame, broker=broker, symbol=job.symbol))
    return written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run terminal-side MT5 exporter and convert binary dumps to parquet.')
    parser.add_argument('--terminal-path', required=True)
    parser.add_argument('--broker', default='bybit_mt5')
    parser.add_argument('--common-root', default=str(default_common_root()))
    parser.add_argument('--from', dest='started_at', required=True)
    parser.add_argument('--to', dest='ended_at', required=True)
    parser.add_argument('--symbol', action='append', required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    started_at = datetime.fromisoformat(args.started_at).replace(tzinfo=UTC)
    ended_at = datetime.fromisoformat(args.ended_at).replace(tzinfo=UTC)
    common_root = Path(args.common_root)
    jobs = [
        ExportJob(
            symbol=symbol,
            timeframe='M5',
            started_at=started_at,
            ended_at=ended_at,
            output_name=f"{symbol.replace('+', 'plus')}_M5_{started_at.strftime('%Y%m%d')}_{ended_at.strftime('%Y%m%d')}.bin",
        )
        for symbol in args.symbol
    ]

    write_job_manifest(common_root=common_root, jobs=jobs)
    config_path = Path.cwd() / 'codex_export_run.ini'
    write_startup_config(config_path=config_path, chart_symbol=jobs[0].symbol)
    exit_code = run_terminal_export(terminal_path=Path(args.terminal_path), config_path=config_path)
    written = decode_exports(common_root=common_root, broker=args.broker, jobs=jobs)

    print(f'terminal_exit={exit_code}')
    for path in written:
        print(path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())