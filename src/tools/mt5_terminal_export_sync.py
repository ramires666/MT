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


@dataclass(slots=True)
class ExportStatus:
    symbol: str
    status: str
    bars: int
    detail: str

    @property
    def ok(self) -> bool:
        return self.status == "ok"


STATUS_FILE_NAME = "history_status.txt"


def default_common_root() -> Path:
    return Path.home() / "AppData" / "Roaming" / "MetaQuotes" / "Terminal" / "Common" / "Files"


def codex_root(common_root: Path) -> Path:
    return common_root / "Codex"


def export_root(common_root: Path) -> Path:
    return codex_root(common_root) / "exports"


def status_path(common_root: Path) -> Path:
    return codex_root(common_root) / STATUS_FILE_NAME


def write_job_manifest(common_root: Path, jobs: list[ExportJob]) -> Path:
    codex_dir = codex_root(common_root)
    codex_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = codex_dir / "history_job.txt"
    lines = [
        f"{job.symbol}|{job.timeframe}|{job.started_at.strftime('%Y.%m.%d %H:%M:%S')}|{job.ended_at.strftime('%Y.%m.%d %H:%M:%S')}|Codex\\exports\\{job.output_name}"
        for job in jobs
    ]
    manifest_path.write_text("\n".join(lines) + "\n", encoding="utf-16")
    return manifest_path


def write_startup_config(config_path: Path, chart_symbol: str, script_name: str = "CodexHistoryExport") -> Path:
    lines = [
        "[StartUp]",
        f"Script={script_name}",
        f"Symbol={chart_symbol}",
        "Period=M5",
        "ShutdownTerminal=1",
    ]
    config_path.write_text("\r\n".join(lines) + "\r\n", encoding="utf-8")
    return config_path


def run_terminal_export(terminal_path: Path, config_path: Path) -> int:
    completed = subprocess.run([str(terminal_path), f"/config:{config_path}"], check=False)
    return completed.returncode


def read_export_statuses(common_root: Path) -> dict[str, ExportStatus]:
    path = status_path(common_root)
    if not path.exists():
        return {}
    statuses: dict[str, ExportStatus] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not raw_line.strip():
            continue
        parts = raw_line.split("|", 3)
        if len(parts) != 4:
            continue
        symbol, status, bars_text, detail = parts
        try:
            bars = int(bars_text)
        except ValueError:
            bars = 0
        statuses[symbol] = ExportStatus(symbol=symbol, status=status, bars=bars, detail=detail)
    return statuses


def _export_file_name(job: ExportJob, statuses: dict[str, ExportStatus]) -> str:
    status = statuses.get(job.symbol)
    if status and status.detail:
        detail = status.detail.replace("\\", "/")
        if detail.lower().endswith(".bin"):
            return Path(detail).name
    return job.output_name


def decode_exports(common_root: Path, broker: str, jobs: list[ExportJob]) -> list[Path]:
    written: list[Path] = []
    statuses = read_export_statuses(common_root)
    root = export_root(common_root)
    for job in jobs:
        status = statuses.get(job.symbol)
        if status is not None and not status.ok:
            continue
        file_name = _export_file_name(job, statuses)
        binary_path = root / file_name
        if not binary_path.exists():
            continue
        frame = read_codex_binary(binary_path)
        written.extend(write_m5_quotes(frame=frame, broker=broker, symbol=job.symbol))
    return written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run terminal-side MT5 exporter and convert binary dumps to parquet.")
    parser.add_argument("--terminal-path", required=True)
    parser.add_argument("--broker", default="bybit_mt5")
    parser.add_argument("--common-root", default=str(default_common_root()))
    parser.add_argument("--from", dest="started_at", required=True)
    parser.add_argument("--to", dest="ended_at", required=True)
    parser.add_argument("--symbol", action="append", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    started_at = datetime.fromisoformat(args.started_at).replace(tzinfo=UTC)
    ended_at = datetime.fromisoformat(args.ended_at).replace(tzinfo=UTC)
    common_root = Path(args.common_root)
    jobs = [
        ExportJob(
            symbol=symbol,
            timeframe="M5",
            started_at=started_at,
            ended_at=ended_at,
            output_name=f"{symbol.replace('+', 'plus')}_M5_{started_at.strftime('%Y%m%d')}_{ended_at.strftime('%Y%m%d')}.bin",
        )
        for symbol in args.symbol
    ]

    write_job_manifest(common_root=common_root, jobs=jobs)
    config_path = Path.cwd() / "codex_export_run.ini"
    write_startup_config(config_path=config_path, chart_symbol=jobs[0].symbol)
    exit_code = run_terminal_export(terminal_path=Path(args.terminal_path), config_path=config_path)
    statuses = read_export_statuses(common_root)
    written = decode_exports(common_root=common_root, broker=args.broker, jobs=jobs)

    print(f"terminal_exit={exit_code}")
    for job in jobs:
        status = statuses.get(job.symbol)
        if status is None:
            print(f"status={job.symbol}|missing|0|status file entry not found")
        else:
            print(f"status={status.symbol}|{status.status}|{status.bars}|{status.detail}")
    for path in written:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
