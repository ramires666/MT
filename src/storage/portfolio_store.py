from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import hashlib
from pathlib import Path

from domain.backtest.distance import DistanceParameters
from domain.contracts import StrategyDefaults, Timeframe
from domain.wfa_serialization import serialize_time
from storage.paths import portfolio_root

PORTFOLIO_FIELDNAMES = (
    "item_id",
    "item_signature",
    "saved_at",
    "source_kind",
    "symbol_1",
    "symbol_2",
    "timeframe",
    "algorithm",
    "lookback_bars",
    "entry_z",
    "exit_z",
    "stop_z",
    "bollinger_k",
    "initial_capital",
    "leverage",
    "margin_budget_per_leg",
    "slippage_points",
    "fee_mode",
    "oos_started_at",
    "context_started_at",
    "context_ended_at",
)


@dataclass(slots=True)
class PortfolioItem:
    item_id: str
    item_signature: str
    saved_at: datetime
    source_kind: str
    symbol_1: str
    symbol_2: str
    timeframe: Timeframe
    algorithm: str
    lookback_bars: int
    entry_z: float
    exit_z: float
    stop_z: float | None
    bollinger_k: float
    initial_capital: float
    leverage: float
    margin_budget_per_leg: float
    slippage_points: float
    fee_mode: str
    oos_started_at: datetime | None = None
    context_started_at: datetime | None = None
    context_ended_at: datetime | None = None

    def params(self) -> DistanceParameters:
        return DistanceParameters(
            lookback_bars=int(self.lookback_bars),
            entry_z=float(self.entry_z),
            exit_z=float(self.exit_z),
            stop_z=None if self.stop_z is None else float(self.stop_z),
            bollinger_k=float(self.bollinger_k),
        )

    def defaults(self) -> StrategyDefaults:
        return StrategyDefaults(
            initial_capital=float(self.initial_capital),
            leverage=float(self.leverage),
            margin_budget_per_leg=float(self.margin_budget_per_leg),
            slippage_points=float(self.slippage_points),
        )


def portfolio_items_path() -> Path:
    return portfolio_root() / "portfolio_items.csv"


def _safe_float(value: object, fallback: float = 0.0) -> float:
    if value in (None, ""):
        return float(fallback)
    return float(value)


def _safe_int(value: object, fallback: int = 0) -> int:
    if value in (None, ""):
        return int(fallback)
    return int(value)


def _parse_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _format_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{float(value):.12g}"


def portfolio_item_signature(
    *,
    symbol_1: str,
    symbol_2: str,
    timeframe: Timeframe,
    params: DistanceParameters,
) -> str:
    payload = "|".join(
        [
            symbol_1,
            symbol_2,
            timeframe.value,
            str(int(params.lookback_bars)),
            _format_float(params.entry_z),
            _format_float(params.exit_z),
            "none" if params.stop_z is None else _format_float(params.stop_z),
            _format_float(params.bollinger_k),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def build_portfolio_item(
    *,
    symbol_1: str,
    symbol_2: str,
    timeframe: Timeframe,
    params: DistanceParameters,
    defaults: StrategyDefaults,
    fee_mode: str,
    source_kind: str,
    oos_started_at: datetime | None = None,
    context_started_at: datetime | None = None,
    context_ended_at: datetime | None = None,
    saved_at: datetime | None = None,
) -> PortfolioItem:
    signature = portfolio_item_signature(symbol_1=symbol_1, symbol_2=symbol_2, timeframe=timeframe, params=params)
    moment = (saved_at or datetime.now(UTC)).astimezone(UTC)
    return PortfolioItem(
        item_id=signature,
        item_signature=signature,
        saved_at=moment,
        source_kind=str(source_kind or "tester"),
        symbol_1=str(symbol_1),
        symbol_2=str(symbol_2),
        timeframe=timeframe,
        algorithm="distance",
        lookback_bars=int(params.lookback_bars),
        entry_z=float(params.entry_z),
        exit_z=float(params.exit_z),
        stop_z=None if params.stop_z is None else float(params.stop_z),
        bollinger_k=float(params.bollinger_k),
        initial_capital=float(defaults.initial_capital),
        leverage=float(defaults.leverage),
        margin_budget_per_leg=float(defaults.margin_budget_per_leg),
        slippage_points=float(defaults.slippage_points),
        fee_mode=str(fee_mode or ""),
        oos_started_at=oos_started_at,
        context_started_at=context_started_at,
        context_ended_at=context_ended_at,
    )


def _row_to_item(row: dict[str, str]) -> PortfolioItem:
    timeframe_value = str(row.get("timeframe") or Timeframe.M15.value)
    return PortfolioItem(
        item_id=str(row.get("item_id") or ""),
        item_signature=str(row.get("item_signature") or row.get("item_id") or ""),
        saved_at=_parse_datetime(row.get("saved_at")) or datetime.now(UTC),
        source_kind=str(row.get("source_kind") or "tester"),
        symbol_1=str(row.get("symbol_1") or ""),
        symbol_2=str(row.get("symbol_2") or ""),
        timeframe=Timeframe(timeframe_value),
        algorithm=str(row.get("algorithm") or "distance"),
        lookback_bars=_safe_int(row.get("lookback_bars"), 96),
        entry_z=_safe_float(row.get("entry_z"), 2.0),
        exit_z=_safe_float(row.get("exit_z"), 0.5),
        stop_z=None if row.get("stop_z") in (None, "") else _safe_float(row.get("stop_z")),
        bollinger_k=_safe_float(row.get("bollinger_k"), 2.0),
        initial_capital=_safe_float(row.get("initial_capital"), 10_000.0),
        leverage=_safe_float(row.get("leverage"), 100.0),
        margin_budget_per_leg=_safe_float(row.get("margin_budget_per_leg"), 500.0),
        slippage_points=_safe_float(row.get("slippage_points"), 1.0),
        fee_mode=str(row.get("fee_mode") or ""),
        oos_started_at=_parse_datetime(row.get("oos_started_at")),
        context_started_at=_parse_datetime(row.get("context_started_at")),
        context_ended_at=_parse_datetime(row.get("context_ended_at")),
    )


def _item_to_row(item: PortfolioItem) -> dict[str, str]:
    payload = asdict(item)
    payload["timeframe"] = item.timeframe.value
    payload["saved_at"] = serialize_time(item.saved_at)
    payload["oos_started_at"] = "" if item.oos_started_at is None else serialize_time(item.oos_started_at)
    payload["context_started_at"] = "" if item.context_started_at is None else serialize_time(item.context_started_at)
    payload["context_ended_at"] = "" if item.context_ended_at is None else serialize_time(item.context_ended_at)
    payload["stop_z"] = "" if item.stop_z is None else _format_float(item.stop_z)
    for key in (
        "entry_z",
        "exit_z",
        "bollinger_k",
        "initial_capital",
        "leverage",
        "margin_budget_per_leg",
        "slippage_points",
    ):
        payload[key] = _format_float(payload[key])  # type: ignore[index]
    payload["lookback_bars"] = str(int(item.lookback_bars))
    return {field: str(payload.get(field, "") or "") for field in PORTFOLIO_FIELDNAMES}


def load_portfolio_items() -> list[PortfolioItem]:
    path = portfolio_items_path()
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        items = [_row_to_item(row) for row in reader if row]
    items.sort(key=lambda item: (item.saved_at, item.symbol_1, item.symbol_2))
    return items


def _write_portfolio_items(items: list[PortfolioItem]) -> None:
    path = portfolio_items_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(PORTFOLIO_FIELDNAMES))
        writer.writeheader()
        for item in items:
            writer.writerow(_item_to_row(item))


def upsert_portfolio_item(item: PortfolioItem) -> tuple[PortfolioItem, bool]:
    items = load_portfolio_items()
    created = True
    for index, existing in enumerate(items):
        if existing.item_signature != item.item_signature:
            continue
        item.item_id = existing.item_id
        item.item_signature = existing.item_signature
        items[index] = item
        created = False
        break
    if created:
        items.append(item)
    items.sort(key=lambda current: (current.saved_at, current.symbol_1, current.symbol_2))
    _write_portfolio_items(items)
    return item, created


def remove_portfolio_items(item_ids: list[str]) -> int:
    if not item_ids:
        return 0
    id_set = {str(item_id) for item_id in item_ids}
    items = load_portfolio_items()
    kept = [item for item in items if item.item_id not in id_set]
    removed = len(items) - len(kept)
    if removed <= 0:
        return 0
    _write_portfolio_items(kept)
    return removed
