from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import polars as pl
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.tree import DecisionTreeRegressor

from domain.backtest.distance import DistanceParameters, run_distance_backtest_frame
from domain.contracts import PairSelection, StrategyDefaults, Timeframe
from domain.data.io import load_instrument_spec
from domain.optimizer.distance import load_pair_frame
from storage.paths import meta_selector_root
from storage.wfa_results import load_wfa_optimization_history, wfa_pair_history_path

SUPPORTED_META_MODELS = ("decision_tree", "random_forest", "xgboost")
DEFAULT_META_TARGET = "test_score_log_trades"

FEATURE_COLUMNS = [
    "lookback_bars",
    "entry_z",
    "exit_z",
    "stop_enabled",
    "stop_z_value",
    "bollinger_k",
    "train_objective_score",
    "train_net_profit",
    "train_ending_equity",
    "train_max_drawdown",
    "train_pnl_to_maxdd",
    "train_omega_ratio",
    "train_k_ratio",
    "train_score_log_trades",
    "train_ulcer_index",
    "train_ulcer_performance",
    "train_trades",
    "train_win_rate",
    "train_gross_profit",
    "train_spread_cost",
    "train_slippage_cost",
    "train_commission_cost",
    "train_total_cost",
]

PARAMETER_COLUMNS = [
    "lookback_bars",
    "entry_z",
    "exit_z",
    "stop_enabled",
    "stop_z_value",
    "bollinger_k",
]

NUMERIC_FILL_COLUMNS = FEATURE_COLUMNS + [
    "test_score_log_trades",
    "test_net_profit",
    "test_max_drawdown",
    "test_ending_equity",
    "test_pnl_to_maxdd",
    "test_omega_ratio",
    "test_k_ratio",
    "test_ulcer_index",
    "test_ulcer_performance",
    "test_trades",
    "test_win_rate",
    "test_total_cost",
    "test_commission_cost",
    "test_spread_cost",
    "test_slippage_cost",
]


@dataclass(slots=True)
class MetaSelectorResult:
    status: str
    model_type: str
    target_metric: str
    pair: dict[str, Any]
    timeframe: str
    history_path: str | None
    output_dir: str | None
    total_rows: int
    train_rows: int
    validation_rows: int
    oos_rows: int
    unique_folds: int
    selected_fold_count: int
    oos_started_at: str | None
    model_config: dict[str, Any]
    validation_mae: float | None
    validation_r2: float | None
    ranking_rows: list[dict[str, Any]]
    selected_folds: list[dict[str, Any]]
    stitched_equity: list[dict[str, Any]]
    feature_columns: list[str]
    stitched_net_profit: float = 0.0
    stitched_max_drawdown: float = 0.0
    stitched_total_trades: int = 0
    stitched_total_commission: float = 0.0
    stitched_total_cost: float = 0.0
    failure_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "model_type": self.model_type,
            "target_metric": self.target_metric,
            "pair": self.pair,
            "timeframe": self.timeframe,
            "history_path": self.history_path,
            "output_dir": self.output_dir,
            "total_rows": self.total_rows,
            "train_rows": self.train_rows,
            "validation_rows": self.validation_rows,
            "oos_rows": self.oos_rows,
            "unique_folds": self.unique_folds,
            "selected_fold_count": self.selected_fold_count,
            "oos_started_at": self.oos_started_at,
            "model_config": self.model_config,
            "validation_mae": self.validation_mae,
            "validation_r2": self.validation_r2,
            "ranking_rows": self.ranking_rows,
            "selected_folds": self.selected_folds,
            "stitched_equity": self.stitched_equity,
            "feature_columns": self.feature_columns,
            "stitched_net_profit": self.stitched_net_profit,
            "stitched_max_drawdown": self.stitched_max_drawdown,
            "stitched_total_trades": self.stitched_total_trades,
            "stitched_total_commission": self.stitched_total_commission,
            "stitched_total_cost": self.stitched_total_cost,
            "failure_reason": self.failure_reason,
        }


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


def _with_time_columns(frame: pl.DataFrame) -> pl.DataFrame:
    result = frame
    for column in ("train_started_at", "train_ended_at", "test_started_at", "test_ended_at"):
        if column not in result.columns:
            continue
        dtype = result.schema[column]
        if dtype == pl.Utf8:
            result = result.with_columns(
                pl.col(column).str.to_datetime(strict=False, time_zone="UTC").alias(column)
            )
    return result


def _with_engineered_columns(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    result = _with_time_columns(frame)
    if "stop_enabled" not in result.columns:
        if "stop_z_value" in result.columns:
            result = result.with_columns(pl.col("stop_z_value").is_not_null().alias("stop_enabled"))
        elif "stop_z" in result.columns:
            result = result.with_columns(pl.col("stop_z").is_not_null().alias("stop_enabled"))
        else:
            result = result.with_columns(pl.lit(False).alias("stop_enabled"))
    if "stop_z_value" not in result.columns:
        if "stop_z" in result.columns:
            result = result.with_columns(pl.col("stop_z").cast(pl.Float64).alias("stop_z_value"))
        else:
            result = result.with_columns(pl.lit(None, dtype=pl.Float64).alias("stop_z_value"))
    for column in NUMERIC_FILL_COLUMNS:
        if column not in result.columns:
            result = result.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))
    result = result.with_columns([
        pl.col("stop_enabled").cast(pl.Int8).alias("stop_enabled"),
        pl.col("lookback_bars").cast(pl.Float64).alias("lookback_bars"),
        pl.col("train_trades").cast(pl.Float64).alias("train_trades"),
        pl.col("test_trades").cast(pl.Float64).alias("test_trades"),
    ])
    fill_exprs = [
        pl.col(column).cast(pl.Float64).fill_null(0.0).fill_nan(0.0).alias(column)
        for column in NUMERIC_FILL_COLUMNS
    ]
    return result.with_columns(fill_exprs)


def _normalized_model_config(model_type: str, config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    source = dict(config or {})
    if model_type == "decision_tree":
        return {
            "max_depth": int(source.get("max_depth", 5) or 5),
            "min_samples_leaf": int(source.get("min_samples_leaf", 6) or 6),
        }
    if model_type == "random_forest":
        max_features = source.get("max_features", "sqrt") or "sqrt"
        return {
            "n_estimators": int(source.get("n_estimators", 300) or 300),
            "max_depth": int(source.get("max_depth", 6) or 6),
            "min_samples_leaf": int(source.get("min_samples_leaf", 4) or 4),
            "max_features": str(max_features),
        }
    if model_type == "xgboost":
        return {
            "n_estimators": int(source.get("n_estimators", 240) or 240),
            "max_depth": int(source.get("max_depth", 4) or 4),
            "learning_rate": float(source.get("learning_rate", 0.05) or 0.05),
            "subsample": float(source.get("subsample", 0.9) or 0.9),
            "colsample_bytree": float(source.get("colsample_bytree", 0.9) or 0.9),
        }
    raise ValueError(f"Unsupported meta-selector model: {model_type}")


def _build_model(model_type: str, config: Mapping[str, Any] | None = None):
    normalized = _normalized_model_config(model_type, config)
    if model_type == "decision_tree":
        return DecisionTreeRegressor(
            max_depth=int(normalized["max_depth"]),
            min_samples_leaf=int(normalized["min_samples_leaf"]),
            random_state=17,
        )
    if model_type == "random_forest":
        return RandomForestRegressor(
            n_estimators=int(normalized["n_estimators"]),
            max_depth=int(normalized["max_depth"]),
            min_samples_leaf=int(normalized["min_samples_leaf"]),
            max_features=str(normalized["max_features"]),
            random_state=17,
            n_jobs=1,
        )
    if model_type == "xgboost":
        try:
            from xgboost import XGBRegressor
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError("xgboost is not installed in the active Python environment.") from exc
        return XGBRegressor(
            n_estimators=int(normalized["n_estimators"]),
            max_depth=int(normalized["max_depth"]),
            learning_rate=float(normalized["learning_rate"]),
            subsample=float(normalized["subsample"]),
            colsample_bytree=float(normalized["colsample_bytree"]),
            objective="reg:squarederror",
            random_state=17,
            n_jobs=1,
        )
    raise ValueError(f"Unsupported meta-selector model: {model_type}")


def _window_key_expr() -> pl.Expr:
    return pl.concat_str([
        pl.col("test_started_at").dt.strftime("%Y-%m-%dT%H:%M:%S"),
        pl.lit("__"),
        pl.col("test_ended_at").dt.strftime("%Y-%m-%dT%H:%M:%S"),
    ]).alias("window_key")


def _with_window_key(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    result = _with_time_columns(frame)
    if "window_key" in result.columns:
        return result
    if "test_started_at" in result.columns and "test_ended_at" in result.columns:
        return result.with_columns(_window_key_expr())
    return result


def _validation_split(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    keyed = _with_window_key(frame)
    if keyed.is_empty() or "window_key" not in keyed.columns:
        return keyed, pl.DataFrame(schema=keyed.schema)
    windows = (
        keyed
        .select(["window_key", "test_started_at"])
        .unique(maintain_order=True)
        .sort("test_started_at")
    )
    window_keys = [value for value in windows.get_column("window_key").to_list() if value is not None]
    if len(window_keys) < 4 or keyed.height < 12:
        return keyed, pl.DataFrame(schema=keyed.schema)
    holdout_count = max(1, int(np.ceil(len(window_keys) * 0.25)))
    holdout_windows = set(window_keys[-holdout_count:])
    validation = keyed.filter(pl.col("window_key").is_in(sorted(holdout_windows)))
    training = keyed.filter(~pl.col("window_key").is_in(sorted(holdout_windows)))
    if training.is_empty() or validation.is_empty():
        return keyed, pl.DataFrame(schema=keyed.schema)
    return training, validation


def _fit_predict(
    training_frame: pl.DataFrame,
    scoring_frame: pl.DataFrame,
    *,
    model_type: str,
    target_metric: str,
    model_config: Mapping[str, Any] | None = None,
) -> tuple[np.ndarray, int, int, float | None, float | None]:
    training = _with_engineered_columns(training_frame)
    scoring = _with_engineered_columns(scoring_frame)
    if target_metric not in training.columns:
        raise ValueError(f"Target metric '{target_metric}' is not available in WFA history.")
    if training.height < 4:
        raise ValueError("Meta Selector needs at least 4 pre-OOS optimization rows.")
    X_score = scoring.select(FEATURE_COLUMNS).to_numpy() if not scoring.is_empty() else np.empty((0, len(FEATURE_COLUMNS)))

    split_train, split_validation = _validation_split(training)
    validation_mae = None
    validation_r2 = None
    train_rows = training.height
    validation_rows = 0
    if not split_validation.is_empty() and split_train.height >= 3:
        model = _build_model(model_type, model_config)
        X_train = split_train.select(FEATURE_COLUMNS).to_numpy()
        y_train = split_train.get_column(target_metric).to_numpy()
        X_validation = split_validation.select(FEATURE_COLUMNS).to_numpy()
        y_validation = split_validation.get_column(target_metric).to_numpy()
        model.fit(X_train, y_train)
        validation_pred = model.predict(X_validation)
        validation_mae = float(mean_absolute_error(y_validation, validation_pred))
        validation_r2 = float(r2_score(y_validation, validation_pred)) if len(y_validation) > 1 else None
        train_rows = split_train.height
        validation_rows = split_validation.height

    final_model = _build_model(model_type, model_config)
    final_model.fit(training.select(FEATURE_COLUMNS).to_numpy(), training.get_column(target_metric).to_numpy())
    predictions = np.asarray(final_model.predict(X_score), dtype=np.float64) if scoring.height else np.asarray([], dtype=np.float64)
    return predictions, train_rows, validation_rows, validation_mae, validation_r2


def _rank_parameter_sets(frame: pl.DataFrame, predictions: np.ndarray) -> list[dict[str, Any]]:
    enriched = _with_window_key(_with_engineered_columns(frame)).with_columns(pl.Series("predicted_target", predictions))
    grouped = (
        enriched.group_by(PARAMETER_COLUMNS)
        .agg(
            pl.len().alias("rows"),
            pl.col("fold").n_unique().alias("folds"),
            pl.col("predicted_target").mean().alias("predicted_mean"),
            pl.col("predicted_target").std(ddof=0).fill_null(0.0).alias("predicted_std"),
            pl.col("test_score_log_trades").mean().alias("actual_test_score_mean"),
            pl.col("test_net_profit").mean().alias("actual_test_net_mean"),
            pl.col("test_max_drawdown").mean().alias("actual_test_maxdd_mean"),
            pl.col("test_trades").mean().alias("actual_test_trades_mean"),
            pl.col("train_score_log_trades").mean().alias("train_score_mean"),
            pl.col("train_net_profit").mean().alias("train_net_mean"),
            pl.col("train_pnl_to_maxdd").mean().alias("train_pnl_to_maxdd_mean"),
        )
        .with_columns(
            (
                pl.col("predicted_mean")
                - (pl.col("predicted_std") * 0.50)
                + (pl.col("train_pnl_to_maxdd_mean") * 0.05)
            ).alias("stability_score")
        )
        .sort(["stability_score", "predicted_mean", "train_score_mean", "train_net_mean"], descending=[True, True, True, True])
    )
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(grouped.iter_rows(named=True), start=1):
        stop_enabled = bool(int(row.get("stop_enabled", 0) or 0))
        stop_value = row.get("stop_z_value")
        rows.append(
            {
                "rank": index,
                "lookback_bars": int(round(float(row.get("lookback_bars", 0.0) or 0.0))),
                "entry_z": float(row.get("entry_z", 0.0) or 0.0),
                "exit_z": float(row.get("exit_z", 0.0) or 0.0),
                "stop_z": None if not stop_enabled else float(stop_value or 0.0),
                "stop_z_label": "disabled" if not stop_enabled else f"{float(stop_value or 0.0):.2f}",
                "bollinger_k": float(row.get("bollinger_k", 0.0) or 0.0),
                "rows": int(row.get("rows", 0) or 0),
                "folds": int(row.get("folds", 0) or 0),
                "predicted_mean": float(row.get("predicted_mean", 0.0) or 0.0),
                "predicted_std": float(row.get("predicted_std", 0.0) or 0.0),
                "stability_score": float(row.get("stability_score", 0.0) or 0.0),
                "actual_test_score_mean": float(row.get("actual_test_score_mean", 0.0) or 0.0),
                "actual_test_net_mean": float(row.get("actual_test_net_mean", 0.0) or 0.0),
                "actual_test_maxdd_mean": float(row.get("actual_test_maxdd_mean", 0.0) or 0.0),
                "actual_test_trades_mean": float(row.get("actual_test_trades_mean", 0.0) or 0.0),
                "train_score_mean": float(row.get("train_score_mean", 0.0) or 0.0),
                "train_net_mean": float(row.get("train_net_mean", 0.0) or 0.0),
            }
        )
    return rows


def _select_rows_per_fold(frame: pl.DataFrame, predictions: np.ndarray) -> pl.DataFrame:
    enriched = _with_window_key(_with_engineered_columns(frame)).with_columns(pl.Series("predicted_target", predictions))
    if enriched.is_empty():
        return enriched
    return (
        enriched
        .sort(
            ["test_started_at", "predicted_target", "train_score_log_trades", "train_net_profit", "train_pnl_to_maxdd", "trial_id"],
            descending=[False, True, True, True, True, False],
        )
        .group_by("window_key", maintain_order=True)
        .first()
        .sort("test_started_at")
    )


def _stitch_equity_chunks(chunks: list[dict[str, Any]], initial_capital: float) -> list[dict[str, Any]]:
    stitched: list[dict[str, Any]] = []
    current_equity = float(initial_capital)
    for chunk in chunks:
        times = list(chunk.get("times", []))
        equities = list(chunk.get("equities", []))
        if not times or not equities:
            continue
        for time_value, equity_value in zip(times, equities):
            adjusted = current_equity + (float(equity_value) - float(initial_capital))
            stitched.append({"time": time_value, "equity": round(adjusted, 6)})
        current_equity = stitched[-1]["equity"]
    return stitched


def _max_drawdown(points: list[dict[str, Any]]) -> float:
    if not points:
        return 0.0
    peak = float(points[0]["equity"])
    max_dd = 0.0
    for point in points:
        equity = float(point["equity"])
        if equity > peak:
            peak = equity
        drawdown = equity - peak
        if drawdown < max_dd:
            max_dd = drawdown
    return float(max_dd)


def _build_selected_fold_outputs(
    *,
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    defaults: StrategyDefaults,
    selected: pl.DataFrame,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], float, float, int, float, float]:
    if selected.is_empty():
        return [], [], 0.0, 0.0, 0, 0.0, 0.0
    spec_1 = load_instrument_spec(broker, pair.symbol_1)
    spec_2 = load_instrument_spec(broker, pair.symbol_2)
    selected_rows: list[dict[str, Any]] = []
    stitched_chunks: list[dict[str, Any]] = []
    total_net = 0.0
    total_trades = 0
    total_commission = 0.0
    total_cost = 0.0

    for row in selected.iter_rows(named=True):
        test_started_at = row.get("test_started_at")
        test_ended_at = row.get("test_ended_at")
        if not isinstance(test_started_at, datetime) or not isinstance(test_ended_at, datetime):
            continue
        params = DistanceParameters(
            lookback_bars=int(round(float(row.get("lookback_bars", 0.0) or 0.0))),
            entry_z=float(row.get("entry_z", 0.0) or 0.0),
            exit_z=float(row.get("exit_z", 0.0) or 0.0),
            stop_z=None if not bool(int(row.get("stop_enabled", 0) or 0)) else float(row.get("stop_z_value", 0.0) or 0.0),
            bollinger_k=float(row.get("bollinger_k", 0.0) or 0.0),
        )
        frame = load_pair_frame(
            broker=broker,
            pair=pair,
            timeframe=timeframe,
            started_at=test_started_at,
            ended_at=test_ended_at,
        )
        if frame.is_empty():
            continue
        result = run_distance_backtest_frame(
            frame=frame,
            pair=pair,
            defaults=defaults,
            params=params,
            point_1=float(spec_1.get("point", 0.0) or 0.0),
            point_2=float(spec_2.get("point", 0.0) or 0.0),
            contract_size_1=float(spec_1.get("contract_size", 1.0) or 1.0),
            contract_size_2=float(spec_2.get("contract_size", 1.0) or 1.0),
            spec_1=spec_1,
            spec_2=spec_2,
        )
        summary = result.summary
        stitched_chunks.append(
            {
                "times": [time_value.isoformat().replace("+00:00", "Z") for time_value in result.frame.get_column("time").to_list()],
                "equities": [float(value) for value in result.frame.get_column("equity_total").to_list()],
            }
        )
        total_net += float(summary.get("net_pnl", 0.0) or 0.0)
        total_trades += int(summary.get("trades", 0) or 0)
        total_commission += float(summary.get("total_commission", 0.0) or 0.0)
        total_cost += float(summary.get("total_cost", 0.0) or 0.0)
        stop_enabled = bool(int(row.get("stop_enabled", 0) or 0))
        stop_value = row.get("stop_z_value")
        selected_rows.append(
            {
                "fold": int(row.get("fold", 0) or 0),
                "test_started_at": _serialize_time(test_started_at),
                "test_ended_at": _serialize_time(test_ended_at),
                "lookback_bars": int(params.lookback_bars),
                "entry_z": float(params.entry_z),
                "exit_z": float(params.exit_z),
                "stop_z": params.stop_z,
                "stop_z_label": "disabled" if not stop_enabled else f"{float(stop_value or 0.0):.2f}",
                "bollinger_k": float(params.bollinger_k),
                "predicted_target": float(row.get("predicted_target", 0.0) or 0.0),
                "test_score": float(row.get("test_score_log_trades", 0.0) or 0.0),
                "test_net_profit": float(summary.get("net_pnl", 0.0) or 0.0),
                "test_max_drawdown": float(summary.get("max_drawdown", 0.0) or 0.0),
                "test_trades": int(summary.get("trades", 0) or 0),
                "test_commission": float(summary.get("total_commission", 0.0) or 0.0),
                "test_total_cost": float(summary.get("total_cost", 0.0) or 0.0),
            }
        )
    stitched_equity = _stitch_equity_chunks(stitched_chunks, defaults.initial_capital)
    return (
        selected_rows,
        stitched_equity,
        round(total_net, 6),
        round(_max_drawdown(stitched_equity), 6),
        int(total_trades),
        round(total_commission, 6),
        round(total_cost, 6),
    )


def _persist_meta_selector_outputs(
    *,
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    model_type: str,
    result: MetaSelectorResult,
) -> Path:
    ranking_path, selected_path, stitched_path, summary_path = _output_paths(broker=broker, pair=pair, timeframe=timeframe, model_type=model_type)
    ranking_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(result.ranking_rows).write_parquet(ranking_path, compression="zstd", statistics=True)
    pl.DataFrame(result.selected_folds).write_parquet(selected_path, compression="zstd", statistics=True)
    pl.DataFrame(result.stitched_equity).write_parquet(stitched_path, compression="zstd", statistics=True)
    summary_payload = result.to_dict()
    summary_payload["saved_at"] = datetime.now(UTC).isoformat()
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    return ranking_path.parent


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
    normalized_model_config = _normalized_model_config(model_type, model_config)
    history_path = wfa_pair_history_path(broker, pair, timeframe)
    history = _with_engineered_columns(load_wfa_optimization_history(broker, pair, timeframe))
    if history.is_empty():
        return MetaSelectorResult(
            status="completed",
            model_type=model_type,
            target_metric=target_metric,
            pair=pair.model_dump(mode="json"),
            timeframe=timeframe.value,
            history_path=str(history_path),
            output_dir=None,
            total_rows=0,
            train_rows=0,
            validation_rows=0,
            oos_rows=0,
            unique_folds=0,
            selected_fold_count=0,
            oos_started_at=_serialize_time(oos_started_at),
            model_config=normalized_model_config,
            validation_mae=None,
            validation_r2=None,
            ranking_rows=[],
            selected_folds=[],
            stitched_equity=[],
            feature_columns=list(FEATURE_COLUMNS),
            failure_reason="no_wfa_history",
        ).to_dict()

    if oos_started_at is not None:
        training_history = history.filter(pl.col("test_started_at") < oos_started_at)
        oos_history = history.filter(pl.col("test_started_at") >= oos_started_at)
    else:
        training_history = history
        oos_history = history

    if training_history.is_empty():
        return MetaSelectorResult(
            status="completed",
            model_type=model_type,
            target_metric=target_metric,
            pair=pair.model_dump(mode="json"),
            timeframe=timeframe.value,
            history_path=str(history_path),
            output_dir=None,
            total_rows=int(history.height),
            train_rows=0,
            validation_rows=0,
            oos_rows=int(oos_history.height),
            unique_folds=int(_with_window_key(history).select(pl.col("window_key").n_unique()).item()) if "test_started_at" in history.columns and "test_ended_at" in history.columns else 0,
            selected_fold_count=0,
            oos_started_at=_serialize_time(oos_started_at),
            model_config=normalized_model_config,
            validation_mae=None,
            validation_r2=None,
            ranking_rows=[],
            selected_folds=[],
            stitched_equity=[],
            feature_columns=list(FEATURE_COLUMNS),
            failure_reason="no_pre_oos_history",
        ).to_dict()

    if oos_history.is_empty():
        return MetaSelectorResult(
            status="completed",
            model_type=model_type,
            target_metric=target_metric,
            pair=pair.model_dump(mode="json"),
            timeframe=timeframe.value,
            history_path=str(history_path),
            output_dir=None,
            total_rows=int(history.height),
            train_rows=int(training_history.height),
            validation_rows=0,
            oos_rows=0,
            unique_folds=int(_with_window_key(history).select(pl.col("window_key").n_unique()).item()) if "test_started_at" in history.columns and "test_ended_at" in history.columns else 0,
            selected_fold_count=0,
            oos_started_at=_serialize_time(oos_started_at),
            model_config=normalized_model_config,
            validation_mae=None,
            validation_r2=None,
            ranking_rows=[],
            selected_folds=[],
            stitched_equity=[],
            feature_columns=list(FEATURE_COLUMNS),
            failure_reason="no_oos_folds_after_cutoff",
        ).to_dict()

    try:
        predictions, train_rows, validation_rows, validation_mae, validation_r2 = _fit_predict(
            training_history,
            oos_history,
            model_type=model_type,
            target_metric=target_metric,
            model_config=normalized_model_config,
        )
        ranking_rows = _rank_parameter_sets(oos_history, predictions)
        selected = _select_rows_per_fold(oos_history, predictions)
        selected_folds, stitched_equity, stitched_net_profit, stitched_max_drawdown, stitched_total_trades, stitched_total_commission, stitched_total_cost = _build_selected_fold_outputs(
            broker=broker,
            pair=pair,
            timeframe=timeframe,
            defaults=effective_defaults,
            selected=selected,
        )
    except Exception as exc:
        return MetaSelectorResult(
            status="completed",
            model_type=model_type,
            target_metric=target_metric,
            pair=pair.model_dump(mode="json"),
            timeframe=timeframe.value,
            history_path=str(history_path),
            output_dir=None,
            total_rows=int(history.height),
            train_rows=0,
            validation_rows=0,
            oos_rows=int(oos_history.height),
            unique_folds=int(_with_window_key(history).select(pl.col("window_key").n_unique()).item()) if "test_started_at" in history.columns and "test_ended_at" in history.columns else 0,
            selected_fold_count=0,
            oos_started_at=_serialize_time(oos_started_at),
            model_config=normalized_model_config,
            validation_mae=None,
            validation_r2=None,
            ranking_rows=[],
            selected_folds=[],
            stitched_equity=[],
            feature_columns=list(FEATURE_COLUMNS),
            failure_reason=str(exc),
        ).to_dict()

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
        model_config=normalized_model_config,
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
