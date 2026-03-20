from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from mt5_gateway.client import MT5Client
from mt5_gateway.service import MT5GatewayService, QuoteDownloadRequest


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MT5 catalog and quote sync")
    parser.add_argument("--terminal-path", required=True)
    parser.add_argument("--broker", default="bybit_mt5")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("catalog")

    quotes_parser = subparsers.add_parser("quotes")
    quotes_parser.add_argument("--symbol", action="append", required=True)
    quotes_parser.add_argument("--from", dest="started_at", required=True)
    quotes_parser.add_argument("--to", dest="ended_at", required=True)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    client = MT5Client(terminal_path=args.terminal_path)
    client.initialize()
    try:
        service = MT5GatewayService(client=client, broker=args.broker)
        if args.command == "catalog":
            output_path = service.refresh_instruments()
            print(output_path)
            return 0

        started_at = _parse_dt(args.started_at)
        ended_at = _parse_dt(args.ended_at)
        for symbol in args.symbol:
            written = service.download_quotes(
                QuoteDownloadRequest(symbol=symbol, started_at=started_at, ended_at=ended_at)
            )
            print(f"{symbol}: {len(written)} parquet partitions")
            for path in written:
                print(path)
        return 0
    finally:
        client.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())