from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

import polars as pl

from domain.contracts import PairSelection, StrategyDefaults, Timeframe
from domain.meta_selector_ml import (
    fit_predict,
    normalized_model_config,
    rank_parameter_sets,
    select_rows_per_fold,
    validation_split,
    with_engineered_columns,
    with_window_key,
)
from domain.meta_selector_outputs import build_selected_fold_outputs
from domain.meta_selector_types import (
    DEFAULT_META_TARGET,
    FEATURE_COLUMNS,
    MetaSelectorResult,
    SUPPORTED_META_MODELS,
)
from storage.paths import meta_selector_root
from storage.wfa_results import load_wfa_optimization_history, wfa_pair_history_path


def _sanitize_symbol(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value or "unknown")


def _pair_key(pair: PairSelection) -> str:
    return f"{_sanitize_symbol(pair.symbol_1)}__{_sanitize_symbol(pair.symbol_2)}"


def _meta_output_dir(*, broker: str, pair: PairSelection, timeframe: Timeframe, model_type: str) -> Path:
    return meta_selector_root() / broker / timeframe.value / _pair_key(pair) / model_type


def _output_paths(*, broker: str, pair: PairSelection, timeframe: Timeframe, model_type: str) -> tuple[Path, Path, Path, Path]:
    output_dir = _meta_output_dir(broker=broker, pair=pair, timeframe=timeframe, model_type=model_type)
    return (
        output_dir / "ranking.parquet",
        output_dir / "selected_folds.parquet",
        output_dir / "stitched_equity.parquet",
        output_dir / "summary.json",
    )


def _serialize_time(moment: datetime | None) -> str | None:
    if moment is None:
        return None
    current = moment if moment.tzinfo else moment.replace(tzinfo=UTC)
    return current.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _validation_split(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    return validation_split(frame)


def _select_rows_per_fold(frame: pl.DataFrame, predictions) -> pl.DataFrame:
    return select_rows_per_fold(frame, predictions)


def _persist_meta_selector_outputs(
    *,
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    model_type: str,
    result: MetaSelectorResult,
) -> Path:
    ranking_path, selected_path, stitched_path, summary_path = _output_paths(
        broker=broker,
        pair=pair,
        timeframe=timeframe,
        model_type=model_type,
    )
    ranking_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(result.ranking_rows).write_parquet(ranking_path, compression="zstd", statistics=True)
    pl.DataFrame(result.selected_folds).write_parquet(selected_path, compression="zstd", statistics=True)
    pl.DataFrame(result.stitched_equity).write_parquet(stitched_path, compression="zstd", statistics=True)
    summary_payload = result.to_dict()
    summary_payload["saved_at"] = datetime.now(UTC).isoformat()
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    return ranking_path.parent


def _empty_result(
    *,
    model_type: str,
    target_metric: str,
    pair: PairSelection,
    timeframe: Timeframe,
    history_path: Path,
    total_rows: int,
    train_rows: int,
    validation_rows: int,
    oos_rows: int,
    unique_folds: int,
    oos_started_at: datetime | None,
    model_config: dict[str, Any],
    failure_reason: str,
) -> dict[str, Any]:
    return MetaSelectorResult(
        status="completed",
        model_type=model_type,
        target_metric=target_metric,
        pair=pair.model_dump(mode="json"),
        timeframe=timeframe.value,
        history_path=str(history_path),
        output_dir=None,
        total_rows=total_rows,
        train_rows=train_rows,
        validation_rows=validation_rows,
        oos_rows=oos_rows,
        unique_folds=unique_folds,
        selected_fold_count=0,
        oos_started_at=_serialize_time(oos_started_at),
        model_config=model_config,
        validation_mae=None,
        validation_r2=None,
        ranking_rows=[],
        selected_folds=[],
        stitched_equity=[],
        feature_columns=list(FEATURE_COLUMNS),
        failure_reason=failure_reason,
    ).to_dict()


def _history_window_count(frame: pl.DataFrame) -> int:
    if "test_started_at" in frame.columns and "test_ended_at" in frame.columns:
        return int(with_window_key(frame).select(pl.col("window_key").n_unique()).item())
    return 0


def run_meta_selector(
    *,
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    model_type: str = "decision_tree",
    target_metric: str = DEFAULT_META_TARGET,
    defaults: StrategyDefaults | None = None,
    oos_started_at: datetime | None = None,
    model_config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if model_type not in SUPPORTED_META_MODELS:
        raise ValueError(f"Unsupported meta-selector model: {model_type}")
    effective_defaults = defaults or StrategyDefaults()
    normalized_config = normalized_model_config(model_type, model_config)
    history_path = wfa_pair_history_path(broker, pair, timeframe)
    history = with_engineered_columns(load_wfa_optimization_history(broker, pair, timeframe))
    if history.is_empty():
        return _empty_result(
            model_type=model_type,
            target_metric=target_metric,
            pair=pair,
            timeframe=timeframe,
            history_path=history_path,
            total_rows=0,
            train_rows=0,
            validation_rows=0,
            oos_rows=0,
            unique_folds=0,
            oos_started_at=oos_started_at,
            model_config=normalized_config,
            failure_reason="no_wfa_history",
        )

    if oos_started_at is not None:
        training_history = history.filter(pl.col("test_started_at") < oos_started_at)
        oos_history = history.filter(pl.col("test_started_at") >= oos_started_at)
    else:
        training_history = history
        oos_history = history

    unique_windows = _history_window_count(history)
    if training_history.is_empty():
        return _empty_result(
            model_type=model_type,
            target_metric=target_metric,
            pair=pair,
            timeframe=timeframe,
            history_path=history_path,
            total_rows=int(history.height),
            train_rows=0,
            validation_rows=0,
            oos_rows=int(oos_history.height),
            unique_folds=unique_windows,
            oos_started_at=oos_started_at,
            model_config=normalized_config,
            failure_reason="no_pre_oos_history",
        )

    if oos_history.is_empty():
        return _empty_result(
            model_type=model_type,
            target_metric=target_metric,
            pair=pair,
            timeframe=timeframe,
            history_path=history_path,
            total_rows=int(history.height),
            train_rows=int(training_history.height),
            validation_rows=0,
            oos_rows=0,
            unique_folds=unique_windows,
            oos_started_at=oos_started_at,
            model_config=normalized_config,
            failure_reason="no_oos_folds_after_cutoff",
        )

    try:
        predictions, train_rows, validation_rows, validation_mae, validation_r2 = fit_predict(
            training_history,
            oos_history,
            model_type=model_type,
            target_metric=target_metric,
            model_config=normalized_config,
        )
        ranking_rows = rank_parameter_sets(oos_history, predictions)
        selected = _select_rows_per_fold(oos_history, predictions)
        (
            selected_folds,
            stitched_equity,
            stitched_net_profit,
            stitched_max_drawdown,
            stitched_total_trades,
            stitched_total_commission,
            stitched_total_cost,
        ) = build_selected_fold_outputs(
            broker=broker,
            pair=pair,
            timeframe=timeframe,
            defaults=effective_defaults,
            selected=selected,
            serialize_time=_serialize_time,
        )
    except Exception as exc:
        return _empty_result(
            model_type=model_type,
            target_metric=target_metric,
            pair=pair,
            timeframe=timeframe,
            history_path=history_path,
            total_rows=int(history.height),
            train_rows=0,
            validation_rows=0,
            oos_rows=int(oos_history.height),
            unique_folds=unique_windows,
            oos_started_at=oos_started_at,
            model_config=normalized_config,
            failure_reason=str(exc),
        )

    result = MetaSelectorResult(
        status="completed",
        model_type=model_type,
        target_metric=target_metric,
        pair=pair.model_dump(mode="json"),
        timeframe=timeframe.value,
        history_path=str(history_path),
        output_dir=None,
        total_rows=int(history.height),
        train_rows=int(train_rows),
        validation_rows=int(validation_rows),
        oos_rows=int(oos_history.height),
        unique_folds=int(history.select(pl.col("fold").n_unique()).item()) if "fold" in history.columns else 0,
        selected_fold_count=len(selected_folds),
        oos_started_at=_serialize_time(oos_started_at),
        model_config=normalized_config,
        validation_mae=validation_mae,
        validation_r2=validation_r2,
        ranking_rows=ranking_rows,
        selected_folds=selected_folds,
        stitched_equity=stitched_equity,
        feature_columns=list(FEATURE_COLUMNS),
        stitched_net_profit=stitched_net_profit,
        stitched_max_drawdown=stitched_max_drawdown,
        stitched_total_trades=stitched_total_trades,
        stitched_total_commission=stitched_total_commission,
        stitched_total_cost=stitched_total_cost,
        failure_reason=None if selected_folds else "no_selected_folds",
    )
    output_dir = _persist_meta_selector_outputs(
        broker=broker,
        pair=pair,
        timeframe=timeframe,
        model_type=model_type,
        result=result,
    )
    result.output_dir = str(output_dir)
    return result.to_dict()
