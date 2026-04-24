from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import polars as pl

from domain.contracts import ScanUniverseMode, StrategyDefaults, Timeframe
from domain.optimizer.distance_models import DistanceOptimizationRow
from domain.scan.optimizer_grid_scan import (
    combine_optimizer_grid_scan_results,
    OptimizerGridScanResult,
    OptimizerGridScanRow,
    OptimizerGridScanSummary,
    pair_key,
)
from storage.paths import scans_root


SCANNER_RESULTS_FILENAME = "optimizer_scanner_results.parquet"
RECORD_KIND_RESULT_ROW = "result_row"
RECORD_KIND_PAIR_PROGRESS = "pair_progress"

SCANNER_STORE_SCHEMA: dict[str, pl.DataType] = {
    "record_kind": pl.String,
    "broker": pl.String,
    "request_signature": pl.String,
    "scope": pl.String,
    "timeframe": pl.String,
    "universe_mode": pl.String,
    "normalized_group": pl.String,
    "started_at": pl.Datetime(time_zone="UTC"),
    "ended_at": pl.Datetime(time_zone="UTC"),
    "saved_at": pl.Datetime(time_zone="UTC"),
    "pair_index": pl.Int64,
    "pair_symbol_1": pl.String,
    "pair_symbol_2": pl.String,
    "completed_pairs": pl.Int64,
    "total_pairs": pl.Int64,
    "pairs_with_data": pl.Int64,
    "pairs_with_hits": pl.Int64,
    "total_trials_evaluated": pl.Int64,
    "total_rows": pl.Int64,
    "cancelled": pl.Boolean,
    "failure_reason": pl.String,
    "global_rank": pl.Int64,
    "pair_rank": pl.Int64,
    "trial_id": pl.Int64,
    "symbol_1": pl.String,
    "symbol_2": pl.String,
    "objective_metric": pl.String,
    "objective_score": pl.Float64,
    "net_profit": pl.Float64,
    "ending_equity": pl.Float64,
    "max_drawdown": pl.Float64,
    "pnl_to_maxdd": pl.Float64,
    "omega_ratio": pl.Float64,
    "k_ratio": pl.Float64,
    "ulcer_index": pl.Float64,
    "ulcer_performance": pl.Float64,
    "cagr": pl.Float64,
    "cagr_to_ulcer": pl.Float64,
    "r_squared": pl.Float64,
    "hurst_exponent": pl.Float64,
    "calmar": pl.Float64,
    "trades": pl.Int64,
    "win_rate": pl.Float64,
    "lookback_bars": pl.Int64,
    "entry_z": pl.Float64,
    "exit_z": pl.Float64,
    "stop_z": pl.Float64,
    "bollinger_k": pl.Float64,
    "gross_profit": pl.Float64,
    "spread_cost": pl.Float64,
    "slippage_cost": pl.Float64,
    "commission_cost": pl.Float64,
    "total_cost": pl.Float64,
    "initial_capital": pl.Float64,
    "leverage": pl.Float64,
    "margin_budget_per_leg": pl.Float64,
    "slippage_points": pl.Float64,
    "fee_mode": pl.String,
}


@dataclass(slots=True)
class ScannerPairProgressRow:
    pair_index: int
    symbol_1: str
    symbol_2: str
    completed_pairs: int
    total_pairs: int
    pairs_with_data: int
    pairs_with_hits: int
    total_trials_evaluated: int
    total_rows: int
    cancelled: bool = False
    failure_reason: str | None = None


@dataclass(slots=True)
class OptimizerScannerSnapshot:
    broker: str
    request_signature: str
    scopes: list[str]
    rows: pl.DataFrame
    progress: pl.DataFrame
    summary: dict[str, Any]
    store_path: Path
    loaded_at: datetime


@dataclass(slots=True)
class LoadedOptimizerScannerSnapshot:
    search_signature: str
    scope: str | None
    saved_at: datetime | None
    result: OptimizerGridScanResult
    store_path: Path


def scanner_results_path(broker: str) -> Path:
    return scans_root() / broker / SCANNER_RESULTS_FILENAME


def _normalize_scope(scope: str | None) -> str:
    normalized = str(scope or "all").strip()
    return normalized or "all"


def _normalize_signature(signature: Mapping[str, Any] | str) -> str:
    if isinstance(signature, str):
        normalized = signature.strip()
        return normalized or "{}"
    return json.dumps(signature, sort_keys=True, default=str, separators=(",", ":"))


def _normalize_datetime(moment: datetime | None) -> datetime | None:
    if moment is None:
        return None
    current = moment if moment.tzinfo else moment.replace(tzinfo=UTC)
    return current.astimezone(UTC)


def _normalize_enum_value(value: object | None) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "value", value))


def _empty_store_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=SCANNER_STORE_SCHEMA)


def _frame_from_records(records: Sequence[Mapping[str, Any]]) -> pl.DataFrame:
    if not records:
        return _empty_store_frame()
    frame = pl.from_dicts(list(records), schema=SCANNER_STORE_SCHEMA, strict=False)
    for column, dtype in SCANNER_STORE_SCHEMA.items():
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None).cast(dtype).alias(column))
    return frame.select(list(SCANNER_STORE_SCHEMA))


def _load_store_frame(path: Path) -> pl.DataFrame:
    if not path.exists():
        return _empty_store_frame()
    return _frame_from_records(pl.read_parquet(path).to_dicts())


def _write_store_frame(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path, compression="zstd", statistics=True)


def _row_value(record: Any, key: str, default: Any = None) -> Any:
    if isinstance(record, Mapping):
        value = record.get(key, default)
    else:
        value = getattr(record, key, default)
    if value is None:
        return default
    return value


def _optimization_row_to_record(row: Any) -> dict[str, Any]:
    return {
        "trial_id": int(_row_value(row, "trial_id", 0)),
        "objective_metric": str(_row_value(row, "objective_metric", "")),
        "objective_score": float(_row_value(row, "objective_score", 0.0)),
        "net_profit": float(_row_value(row, "net_profit", 0.0)),
        "ending_equity": float(_row_value(row, "ending_equity", 0.0)),
        "max_drawdown": float(_row_value(row, "max_drawdown", 0.0)),
        "pnl_to_maxdd": float(_row_value(row, "pnl_to_maxdd", 0.0)),
        "omega_ratio": float(_row_value(row, "omega_ratio", 0.0)),
        "k_ratio": float(_row_value(row, "k_ratio", 0.0)),
        "ulcer_index": float(_row_value(row, "ulcer_index", 0.0)),
        "ulcer_performance": float(_row_value(row, "ulcer_performance", 0.0)),
        "cagr": float(_row_value(row, "cagr", 0.0)),
        "cagr_to_ulcer": float(_row_value(row, "cagr_to_ulcer", 0.0)),
        "r_squared": float(_row_value(row, "r_squared", 0.0)),
        "hurst_exponent": float(_row_value(row, "hurst_exponent", 0.0)),
        "calmar": float(_row_value(row, "calmar", 0.0)),
        "trades": int(_row_value(row, "trades", 0)),
        "win_rate": float(_row_value(row, "win_rate", 0.0)),
        "lookback_bars": int(_row_value(row, "lookback_bars", 0)),
        "entry_z": float(_row_value(row, "entry_z", 0.0)),
        "exit_z": float(_row_value(row, "exit_z", 0.0)),
        "stop_z": _row_value(row, "stop_z", None),
        "bollinger_k": float(_row_value(row, "bollinger_k", 0.0)),
        "gross_profit": float(_row_value(row, "gross_profit", 0.0)),
        "spread_cost": float(_row_value(row, "spread_cost", 0.0)),
        "slippage_cost": float(_row_value(row, "slippage_cost", 0.0)),
        "commission_cost": float(_row_value(row, "commission_cost", 0.0)),
        "total_cost": float(_row_value(row, "total_cost", 0.0)),
    }


def _coerce_progress_rows(
    pair_progress_rows: Sequence[ScannerPairProgressRow | Mapping[str, Any]] | None,
    result: Any,
) -> list[ScannerPairProgressRow]:
    if pair_progress_rows is not None:
        rows: list[ScannerPairProgressRow] = []
        for item in pair_progress_rows:
            if isinstance(item, ScannerPairProgressRow):
                rows.append(item)
                continue
            rows.append(
                ScannerPairProgressRow(
                    pair_index=int(item["pair_index"]),
                    symbol_1=str(item["symbol_1"]),
                    symbol_2=str(item["symbol_2"]),
                    completed_pairs=int(item["completed_pairs"]),
                    total_pairs=int(item["total_pairs"]),
                    pairs_with_data=int(item["pairs_with_data"]),
                    pairs_with_hits=int(item["pairs_with_hits"]),
                    total_trials_evaluated=int(item["total_trials_evaluated"]),
                    total_rows=int(item["total_rows"]),
                    cancelled=bool(item.get("cancelled", False)),
                    failure_reason=item.get("failure_reason"),
                )
            )
        return rows

    summary = getattr(result, "summary", None)
    rows = list(getattr(result, "rows", []))
    total_pairs = int(getattr(summary, "total_pairs_evaluated", len(rows)) or len(rows))
    pairs_with_data = int(getattr(summary, "pairs_with_data", len(rows)) or len(rows))
    pairs_with_hits = int(getattr(summary, "pairs_with_hits", len(rows)) or len(rows))
    total_trials = int(getattr(summary, "total_trials_evaluated", len(rows)) or len(rows))
    total_rows = int(getattr(summary, "total_rows", len(rows)) or len(rows))
    cancelled = bool(getattr(result, "cancelled", False))
    failure_reason = getattr(result, "failure_reason", None)
    return [
        ScannerPairProgressRow(
            pair_index=index,
            symbol_1=str(getattr(row, "symbol_1", "")),
            symbol_2=str(getattr(row, "symbol_2", "")),
            completed_pairs=index,
            total_pairs=total_pairs,
            pairs_with_data=pairs_with_data,
            pairs_with_hits=pairs_with_hits,
            total_trials_evaluated=total_trials,
            total_rows=total_rows,
            cancelled=cancelled,
            failure_reason=failure_reason,
        )
        for index, row in enumerate(rows, start=1)
    ]


def _result_row_to_record(
    *,
    broker: str,
    request_signature: str,
    scope: str,
    saved_at: datetime,
    row: Any,
    global_rank: int,
    progress_by_pair: dict[tuple[str, str], ScannerPairProgressRow],
    timeframe: str | None,
    universe_mode: str | None,
    normalized_group: str | None,
    started_at: datetime | None,
    ended_at: datetime | None,
) -> dict[str, Any]:
    optimization_row = getattr(row, "optimization_row")
    pair_progress = progress_by_pair.get((str(getattr(row, "symbol_1", "")), str(getattr(row, "symbol_2", ""))))
    base = {
        "record_kind": RECORD_KIND_RESULT_ROW,
        "broker": broker,
        "request_signature": request_signature,
        "scope": scope,
        "timeframe": timeframe,
        "universe_mode": universe_mode,
        "normalized_group": normalized_group,
        "started_at": started_at,
        "ended_at": ended_at,
        "saved_at": saved_at,
        "pair_index": None if pair_progress is None else int(pair_progress.pair_index),
        "pair_symbol_1": str(getattr(row, "symbol_1", "")),
        "pair_symbol_2": str(getattr(row, "symbol_2", "")),
        "completed_pairs": None if pair_progress is None else int(pair_progress.completed_pairs),
        "total_pairs": None if pair_progress is None else int(pair_progress.total_pairs),
        "pairs_with_data": None if pair_progress is None else int(pair_progress.pairs_with_data),
        "pairs_with_hits": None if pair_progress is None else int(pair_progress.pairs_with_hits),
        "total_trials_evaluated": None if pair_progress is None else int(pair_progress.total_trials_evaluated),
        "total_rows": None if pair_progress is None else int(pair_progress.total_rows),
        "cancelled": False if pair_progress is None else bool(pair_progress.cancelled),
        "failure_reason": None if pair_progress is None else pair_progress.failure_reason,
        "global_rank": int(global_rank),
        "pair_rank": int(getattr(row, "pair_rank", 0)),
        "trial_id": int(getattr(optimization_row, "trial_id", 0)),
        "symbol_1": str(getattr(row, "symbol_1", "")),
        "symbol_2": str(getattr(row, "symbol_2", "")),
        "objective_metric": str(getattr(optimization_row, "objective_metric", "")),
        "objective_score": float(getattr(optimization_row, "objective_score", 0.0)),
        "initial_capital": float(getattr(row, "initial_capital", 0.0)),
        "leverage": float(getattr(row, "leverage", 0.0)),
        "margin_budget_per_leg": float(getattr(row, "margin_budget_per_leg", 0.0)),
        "slippage_points": float(getattr(row, "slippage_points", 0.0)),
        "fee_mode": str(getattr(row, "fee_mode", "")),
    }
    base.update(_optimization_row_to_record(optimization_row))
    return base


def _progress_row_to_record(
    *,
    broker: str,
    request_signature: str,
    scope: str,
    saved_at: datetime,
    progress: ScannerPairProgressRow,
    timeframe: str | None,
    universe_mode: str | None,
    normalized_group: str | None,
    started_at: datetime | None,
    ended_at: datetime | None,
) -> dict[str, Any]:
    return {
        "record_kind": RECORD_KIND_PAIR_PROGRESS,
        "broker": broker,
        "request_signature": request_signature,
        "scope": scope,
        "timeframe": timeframe,
        "universe_mode": universe_mode,
        "normalized_group": normalized_group,
        "started_at": started_at,
        "ended_at": ended_at,
        "saved_at": saved_at,
        "pair_index": int(progress.pair_index),
        "pair_symbol_1": progress.symbol_1,
        "pair_symbol_2": progress.symbol_2,
        "completed_pairs": int(progress.completed_pairs),
        "total_pairs": int(progress.total_pairs),
        "pairs_with_data": int(progress.pairs_with_data),
        "pairs_with_hits": int(progress.pairs_with_hits),
        "total_trials_evaluated": int(progress.total_trials_evaluated),
        "total_rows": int(progress.total_rows),
        "cancelled": bool(progress.cancelled),
        "failure_reason": progress.failure_reason,
        "global_rank": None,
        "pair_rank": None,
        "trial_id": None,
        "symbol_1": None,
        "symbol_2": None,
        "objective_metric": None,
        "objective_score": None,
        "net_profit": None,
        "ending_equity": None,
        "max_drawdown": None,
        "pnl_to_maxdd": None,
        "omega_ratio": None,
        "k_ratio": None,
        "ulcer_index": None,
        "ulcer_performance": None,
        "cagr": None,
        "cagr_to_ulcer": None,
        "r_squared": None,
        "hurst_exponent": None,
        "calmar": None,
        "trades": None,
        "win_rate": None,
        "lookback_bars": None,
        "entry_z": None,
        "exit_z": None,
        "stop_z": None,
        "bollinger_k": None,
        "gross_profit": None,
        "spread_cost": None,
        "slippage_cost": None,
        "commission_cost": None,
        "total_cost": None,
        "initial_capital": None,
        "leverage": None,
        "margin_budget_per_leg": None,
        "slippage_points": None,
        "fee_mode": None,
    }


def _sort_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    return frame.sort(
        by=["scope", "record_kind", "pair_index", "global_rank", "pair_rank", "symbol_1", "symbol_2"],
        nulls_last=True,
    )


def _summary_from_frame(frame: pl.DataFrame) -> dict[str, Any]:
    if frame.is_empty():
        return {
            "scope_count": 0,
            "result_rows": 0,
            "progress_rows": 0,
            "processed_pairs": 0,
            "total_pairs": 0,
            "pairs_with_data": 0,
            "pairs_with_hits": 0,
            "total_trials_evaluated": 0,
            "total_rows": 0,
            "cancelled": False,
            "failure_reason": None,
        }

    result_rows = frame.filter(pl.col("record_kind") == RECORD_KIND_RESULT_ROW)
    progress_rows = frame.filter(pl.col("record_kind") == RECORD_KIND_PAIR_PROGRESS)
    latest_progress_by_scope: dict[str, dict[str, Any]] = {}
    if not progress_rows.is_empty():
        for record in progress_rows.sort(
            by=["scope", "completed_pairs", "pair_index", "saved_at"],
            descending=[False, True, True, True],
            nulls_last=True,
        ).to_dicts():
            scope = str(record.get("scope") or "")
            if scope not in latest_progress_by_scope:
                latest_progress_by_scope[scope] = record

    def _sum_latest(column: str) -> int:
        return sum(int(record.get(column) or 0) for record in latest_progress_by_scope.values())

    failure_reason = None
    if latest_progress_by_scope:
        reasons = [str(record.get("failure_reason") or "").strip() for record in latest_progress_by_scope.values()]
        reasons = [reason for reason in reasons if reason]
        if reasons:
            failure_reason = reasons[-1]

    return {
        "scope_count": len(set(frame.get_column("scope").drop_nulls().to_list())),
        "result_rows": result_rows.height,
        "progress_rows": progress_rows.height,
        "processed_pairs": _sum_latest("completed_pairs"),
        "total_pairs": _sum_latest("total_pairs"),
        "pairs_with_data": _sum_latest("pairs_with_data"),
        "pairs_with_hits": _sum_latest("pairs_with_hits"),
        "total_trials_evaluated": _sum_latest("total_trials_evaluated"),
        "total_rows": result_rows.height,
        "cancelled": any(bool(record.get("cancelled")) for record in latest_progress_by_scope.values()),
        "failure_reason": failure_reason,
    }


def persist_optimizer_scanner_scope_snapshot(
    *,
    broker: str,
    request_signature: Mapping[str, Any] | str,
    scope: str,
    result: Any,
    pair_progress_rows: Sequence[ScannerPairProgressRow | Mapping[str, Any]] | None = None,
    timeframe: Any | None = None,
    universe_mode: Any | None = None,
    normalized_group: str | None = None,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    created_at: datetime | None = None,
    replace_scope: bool = True,
) -> Path:
    request_signature_value = _normalize_signature(request_signature)
    scope_value = _normalize_scope(scope)
    saved_at = _normalize_datetime(created_at) or datetime.now(UTC)
    timeframe_value = _normalize_enum_value(timeframe)
    universe_mode_value = _normalize_enum_value(universe_mode)
    progress_rows = _coerce_progress_rows(pair_progress_rows, result)
    result_cancelled = bool(getattr(result, "cancelled", False))
    result_failure_reason = getattr(result, "failure_reason", None)
    if result_cancelled or result_failure_reason is not None:
        progress_rows = [
            ScannerPairProgressRow(
                pair_index=row.pair_index,
                symbol_1=row.symbol_1,
                symbol_2=row.symbol_2,
                completed_pairs=row.completed_pairs,
                total_pairs=row.total_pairs,
                pairs_with_data=row.pairs_with_data,
                pairs_with_hits=row.pairs_with_hits,
                total_trials_evaluated=row.total_trials_evaluated,
                total_rows=row.total_rows,
                cancelled=row.cancelled or result_cancelled,
                failure_reason=row.failure_reason or result_failure_reason,
            )
            for row in progress_rows
        ]
    progress_by_pair = {(row.symbol_1, row.symbol_2): row for row in progress_rows}
    rows = list(getattr(result, "rows", []))
    records: list[dict[str, Any]] = []

    for global_rank, row in enumerate(rows, start=1):
        records.append(
            _result_row_to_record(
                broker=broker,
                request_signature=request_signature_value,
                scope=scope_value,
                saved_at=saved_at,
                row=row,
                global_rank=global_rank,
                progress_by_pair=progress_by_pair,
                timeframe=timeframe_value,
                universe_mode=universe_mode_value,
                normalized_group=normalized_group,
                started_at=_normalize_datetime(started_at),
                ended_at=_normalize_datetime(ended_at),
            )
        )

    for progress in progress_rows:
        records.append(
            _progress_row_to_record(
                broker=broker,
                request_signature=request_signature_value,
                scope=scope_value,
                saved_at=saved_at,
                progress=progress,
                timeframe=timeframe_value,
                universe_mode=universe_mode_value,
                normalized_group=normalized_group,
                started_at=_normalize_datetime(started_at),
                ended_at=_normalize_datetime(ended_at),
            )
        )

    new_frame = _sort_frame(_frame_from_records(records))
    store_path = scanner_results_path(broker)
    existing = _load_store_frame(store_path)
    if replace_scope and not existing.is_empty():
        existing = existing.filter(
            ~(
                (pl.col("broker") == broker)
                & (pl.col("request_signature") == request_signature_value)
                & (pl.col("scope") == scope_value)
            )
        )
    combined = _sort_frame(pl.concat([existing, new_frame], how="diagonal_relaxed"))
    _write_store_frame(store_path, combined)
    return store_path


def _load_snapshot(
    *,
    broker: str,
    request_signature: str,
    frame: pl.DataFrame,
    scopes: list[str],
) -> OptimizerScannerSnapshot | None:
    if frame.is_empty():
        return None
    rows = _sort_frame(frame.filter(pl.col("record_kind") == RECORD_KIND_RESULT_ROW))
    progress = _sort_frame(frame.filter(pl.col("record_kind") == RECORD_KIND_PAIR_PROGRESS))
    summary = _summary_from_frame(frame)
    return OptimizerScannerSnapshot(
        broker=broker,
        request_signature=request_signature,
        scopes=scopes,
        rows=rows,
        progress=progress,
        summary=summary,
        store_path=scanner_results_path(broker),
        loaded_at=datetime.now(UTC),
    )


def load_optimizer_scanner_scope_snapshot(
    *,
    broker: str,
    request_signature: Mapping[str, Any] | str,
    scope: str,
) -> OptimizerScannerSnapshot | None:
    request_signature_value = _normalize_signature(request_signature)
    scope_value = _normalize_scope(scope)
    frame = _load_store_frame(scanner_results_path(broker))
    filtered = frame.filter(
        (pl.col("broker") == broker)
        & (pl.col("request_signature") == request_signature_value)
        & (pl.col("scope") == scope_value)
    )
    return _load_snapshot(
        broker=broker,
        request_signature=request_signature_value,
        frame=filtered,
        scopes=[scope_value] if not filtered.is_empty() else [],
    )


def load_optimizer_scanner_signature_snapshot(
    *,
    broker: str,
    request_signature: Mapping[str, Any] | str,
) -> OptimizerScannerSnapshot | None:
    request_signature_value = _normalize_signature(request_signature)
    frame = _load_store_frame(scanner_results_path(broker))
    filtered = frame.filter(
        (pl.col("broker") == broker)
        & (pl.col("request_signature") == request_signature_value)
    )
    if filtered.is_empty():
        return None
    scopes = sorted({str(scope) for scope in filtered.get_column("scope").drop_nulls().to_list()})
    return _load_snapshot(
        broker=broker,
        request_signature=request_signature_value,
        frame=filtered,
        scopes=scopes,
    )


def build_optimizer_scanner_request_signature(
    *,
    timeframe: Timeframe,
    train_started_at: datetime,
    train_ended_at: datetime,
    oos_started_at: datetime,
    defaults: StrategyDefaults,
    search_space: Mapping[str, Any],
    fee_mode: str,
    min_r_squared: float = 0.9,
    top_n_per_pair: int = 10,
    pair_filter_signature: str = "",
) -> str:
    return _normalize_signature(
        {
            "timeframe": timeframe.value,
            "train_started_at": _normalize_datetime(train_started_at),
            "train_ended_at": _normalize_datetime(train_ended_at),
            "oos_started_at": _normalize_datetime(oos_started_at),
            "defaults": {
                "initial_capital": float(defaults.initial_capital),
                "leverage": float(defaults.leverage),
                "margin_budget_per_leg": float(defaults.margin_budget_per_leg),
                "slippage_points": float(defaults.slippage_points),
            },
            "search_space": search_space,
            "fee_mode": str(fee_mode or ""),
            "min_r_squared": float(min_r_squared),
            "top_n_per_pair": int(top_n_per_pair),
            "pair_filter_signature": str(pair_filter_signature or ""),
        }
    )


def scanner_scope_label(universe_mode: ScanUniverseMode, normalized_group: str | None) -> str:
    normalized = str(normalized_group or "").strip()
    if normalized:
        return normalized
    return universe_mode.value


def clear_optimizer_scanner_scope(
    *,
    broker: str,
    search_signature: Mapping[str, Any] | str,
    scope: str,
) -> Path:
    request_signature_value = _normalize_signature(search_signature)
    scope_value = _normalize_scope(scope)
    store_path = scanner_results_path(broker)
    existing = _load_store_frame(store_path)
    if existing.is_empty():
        return store_path
    filtered = existing.filter(
        ~(
            (pl.col("broker") == broker)
            & (pl.col("request_signature") == request_signature_value)
            & (pl.col("scope") == scope_value)
        )
    )
    _write_store_frame(store_path, _sort_frame(filtered))
    return store_path


def _pair_progress_rows_from_result(result: OptimizerGridScanResult) -> list[ScannerPairProgressRow]:
    processed_keys = [str(item) for item in (result.processed_pair_keys or []) if str(item)]
    summary = result.summary
    total_pairs = int(summary.total_pair_candidates or summary.total_pairs_evaluated or 0)
    if not processed_keys:
        return []
    rows: list[ScannerPairProgressRow] = []
    for index, current_key in enumerate(processed_keys, start=1):
        symbol_1, _separator, symbol_2 = current_key.partition("::")
        rows.append(
            ScannerPairProgressRow(
                pair_index=index,
                symbol_1=str(symbol_1),
                symbol_2=str(symbol_2),
                completed_pairs=index,
                total_pairs=total_pairs,
                pairs_with_data=int(summary.pairs_with_data),
                pairs_with_hits=int(summary.pairs_with_hits),
                total_trials_evaluated=int(summary.total_trials_evaluated),
                total_rows=int(summary.total_rows),
                cancelled=bool(result.cancelled) and index == len(processed_keys),
                failure_reason=result.failure_reason if index == len(processed_keys) else None,
            )
        )
    return rows


def persist_optimizer_scanner_snapshot(
    *,
    broker: str,
    timeframe: Timeframe,
    train_started_at: datetime,
    train_ended_at: datetime,
    oos_started_at: datetime,
    scope: str,
    search_signature: Mapping[str, Any] | str,
    defaults: StrategyDefaults,
    search_space: Mapping[str, Any],
    fee_mode: str,
    result: OptimizerGridScanResult,
    created_at: datetime | None = None,
) -> Path:
    normalized_scope = scanner_scope_label(ScanUniverseMode.GROUP if scope != ScanUniverseMode.ALL.value else ScanUniverseMode.ALL, scope if scope != ScanUniverseMode.ALL.value else None)
    return persist_optimizer_scanner_scope_snapshot(
        broker=broker,
        request_signature=search_signature,
        scope=normalized_scope,
        result=result,
        pair_progress_rows=_pair_progress_rows_from_result(result),
        timeframe=timeframe,
        universe_mode=ScanUniverseMode.GROUP if normalized_scope != ScanUniverseMode.ALL.value else ScanUniverseMode.ALL,
        normalized_group=None if normalized_scope == ScanUniverseMode.ALL.value else normalized_scope,
        started_at=train_started_at,
        ended_at=train_ended_at,
        created_at=created_at,
        replace_scope=True,
    )


def _saved_at_from_frame(frame: pl.DataFrame) -> datetime | None:
    if frame.is_empty() or "saved_at" not in frame.columns:
        return None
    value = frame.get_column("saved_at").drop_nulls().max()
    return _normalize_datetime(value) if isinstance(value, datetime) else None


def _universe_symbols_from_snapshot(snapshot: OptimizerScannerSnapshot) -> list[str]:
    symbols: set[str] = set()
    for frame in (snapshot.rows, snapshot.progress):
        if frame.is_empty():
            continue
        for column in ("symbol_1", "symbol_2", "pair_symbol_1", "pair_symbol_2"):
            if column not in frame.columns:
                continue
            for value in frame.get_column(column).drop_nulls().to_list():
                text = str(value or "").strip()
                if text:
                    symbols.add(text)
    return sorted(symbols)


def _processed_pair_keys_from_snapshot(snapshot: OptimizerScannerSnapshot) -> list[str]:
    if snapshot.progress.is_empty():
        return []
    rows = snapshot.progress.sort("pair_index", nulls_last=True).to_dicts()
    keys: list[str] = []
    for row in rows:
        symbol_1 = str(row.get("pair_symbol_1") or "").strip()
        symbol_2 = str(row.get("pair_symbol_2") or "").strip()
        if symbol_1 and symbol_2:
            scope = str(row.get("scope") or "").strip()
            key = pair_key(symbol_1, symbol_2)
            if len(snapshot.scopes) > 1 and scope:
                keys.append(f"{scope}:{key}")
            else:
                keys.append(key)
    return keys


def _result_from_snapshot(snapshot: OptimizerScannerSnapshot) -> OptimizerGridScanResult:
    rows: list[OptimizerGridScanRow] = []
    for record in snapshot.rows.sort("global_rank", nulls_last=True).to_dicts():
        optimization_row = DistanceOptimizationRow(
            trial_id=int(record.get("trial_id") or 0),
            objective_metric=str(record.get("objective_metric") or "net_profit"),
            objective_score=float(record.get("objective_score") or 0.0),
            net_profit=float(record.get("net_profit") or 0.0),
            ending_equity=float(record.get("ending_equity") or 0.0),
            max_drawdown=float(record.get("max_drawdown") or 0.0),
            pnl_to_maxdd=float(record.get("pnl_to_maxdd") or 0.0),
            omega_ratio=float(record.get("omega_ratio") or 0.0),
            k_ratio=float(record.get("k_ratio") or 0.0),
            ulcer_index=float(record.get("ulcer_index") or 0.0),
            ulcer_performance=float(record.get("ulcer_performance") or 0.0),
            cagr=float(record.get("cagr") or 0.0),
            cagr_to_ulcer=float(record.get("cagr_to_ulcer") or 0.0),
            r_squared=float(record.get("r_squared") or 0.0),
            hurst_exponent=float(record.get("hurst_exponent") or 0.0),
            calmar=float(record.get("calmar") or 0.0),
            trades=int(record.get("trades") or 0),
            win_rate=float(record.get("win_rate") or 0.0),
            lookback_bars=int(record.get("lookback_bars") or 0),
            entry_z=float(record.get("entry_z") or 0.0),
            exit_z=float(record.get("exit_z") or 0.0),
            stop_z=None if record.get("stop_z") is None else float(record.get("stop_z") or 0.0),
            bollinger_k=float(record.get("bollinger_k") or 0.0),
            gross_profit=float(record.get("gross_profit") or 0.0),
            spread_cost=float(record.get("spread_cost") or 0.0),
            slippage_cost=float(record.get("slippage_cost") or 0.0),
            commission_cost=float(record.get("commission_cost") or 0.0),
            total_cost=float(record.get("total_cost") or 0.0),
        )
        rows.append(
            OptimizerGridScanRow(
                symbol_1=str(record.get("symbol_1") or ""),
                symbol_2=str(record.get("symbol_2") or ""),
                pair_rank=int(record.get("pair_rank") or 0),
                optimization_row=optimization_row,
                universe_scope=str(record.get("scope") or ScanUniverseMode.ALL.value),
                timeframe=str(record.get("timeframe") or Timeframe.M15.value),
                initial_capital=float(record.get("initial_capital") or 0.0),
                leverage=float(record.get("leverage") or 0.0),
                margin_budget_per_leg=float(record.get("margin_budget_per_leg") or 0.0),
                slippage_points=float(record.get("slippage_points") or 0.0),
                fee_mode=str(record.get("fee_mode") or ""),
            )
        )

    universe_symbols = _universe_symbols_from_snapshot(snapshot)
    summary_dict = snapshot.summary
    summary = OptimizerGridScanSummary(
        total_symbols_requested=max(len(universe_symbols), int(summary_dict.get("scope_count") or 0)),
        loaded_symbols=len(universe_symbols),
        total_pair_candidates=int(summary_dict.get("total_pairs") or 0),
        total_pairs_evaluated=int(summary_dict.get("processed_pairs") or 0),
        pairs_with_data=int(summary_dict.get("pairs_with_data") or 0),
        pairs_with_hits=int(summary_dict.get("pairs_with_hits") or 0),
        total_rows=len(rows),
        total_trials_evaluated=int(summary_dict.get("total_trials_evaluated") or 0),
        min_r_squared=0.9,
        top_n_per_pair=10,
    )
    failure_reason = str(summary_dict.get("failure_reason") or "").strip() or None
    if rows and failure_reason == "no_rows_passed_filters":
        failure_reason = None
    return OptimizerGridScanResult(
        summary=summary,
        rows=rows,
        universe_symbols=universe_symbols,
        universe_scope=snapshot.scopes[0] if len(snapshot.scopes) == 1 else ScanUniverseMode.ALL.value,
        processed_pair_keys=_processed_pair_keys_from_snapshot(snapshot),
        cancelled=bool(summary_dict.get("cancelled")),
        failure_reason=failure_reason,
    )


def load_optimizer_scanner_snapshot(
    *,
    broker: str,
    search_signature: Mapping[str, Any] | str,
    scope: str | None,
) -> LoadedOptimizerScannerSnapshot | None:
    request_signature_value = _normalize_signature(search_signature)
    if scope is not None:
        raw_snapshot = load_optimizer_scanner_scope_snapshot(
            broker=broker,
            request_signature=request_signature_value,
            scope=scope,
        )
        if raw_snapshot is None:
            return None
        merged_frame = _sort_frame(pl.concat([raw_snapshot.rows, raw_snapshot.progress], how="diagonal_relaxed"))
        return LoadedOptimizerScannerSnapshot(
            search_signature=raw_snapshot.request_signature,
            scope=_normalize_scope(scope),
            saved_at=_saved_at_from_frame(merged_frame),
            result=_result_from_snapshot(raw_snapshot),
            store_path=raw_snapshot.store_path,
        )

    signature_snapshot = load_optimizer_scanner_signature_snapshot(
        broker=broker,
        request_signature=request_signature_value,
    )
    if signature_snapshot is None:
        return None
    scope_snapshots: list[OptimizerScannerSnapshot] = []
    for current_scope in signature_snapshot.scopes:
        current_snapshot = load_optimizer_scanner_scope_snapshot(
            broker=broker,
            request_signature=request_signature_value,
            scope=current_scope,
        )
        if current_snapshot is not None:
            scope_snapshots.append(current_snapshot)
    if not scope_snapshots:
        return None
    combined_result = combine_optimizer_grid_scan_results(
        [_result_from_snapshot(current_snapshot) for current_snapshot in scope_snapshots]
    )
    merged_frame = _sort_frame(
        pl.concat(
            [
                *[current_snapshot.rows for current_snapshot in scope_snapshots],
                *[current_snapshot.progress for current_snapshot in scope_snapshots],
            ],
            how="diagonal_relaxed",
        )
    )
    return LoadedOptimizerScannerSnapshot(
        search_signature=request_signature_value,
        scope=None,
        saved_at=_saved_at_from_frame(merged_frame),
        result=combined_result,
        store_path=signature_snapshot.store_path,
    )
