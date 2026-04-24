from __future__ import annotations

from datetime import UTC
from typing import Any, Mapping

import numpy as np
import polars as pl
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.tree import DecisionTreeRegressor

from domain.meta_selector_types import FEATURE_COLUMNS, NUMERIC_FILL_COLUMNS, PARAMETER_COLUMNS

XGBOOST_EARLY_STOPPING_ROUNDS = 30
MAX_META_NUMERIC_ABS = 1_000_000_000_000.0


def with_time_columns(frame: pl.DataFrame) -> pl.DataFrame:
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


def with_engineered_columns(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    result = with_time_columns(frame)
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
        pl.col(column).cast(pl.Float64).fill_null(0.0).fill_nan(0.0).clip(-MAX_META_NUMERIC_ABS, MAX_META_NUMERIC_ABS).alias(column)
        for column in NUMERIC_FILL_COLUMNS
    ]
    return result.with_columns(fill_exprs)


def _sanitize_numeric_matrix(values: np.ndarray) -> np.ndarray:
    matrix = np.asarray(values, dtype=np.float64)
    matrix = np.nan_to_num(
        matrix,
        nan=0.0,
        posinf=MAX_META_NUMERIC_ABS,
        neginf=-MAX_META_NUMERIC_ABS,
    )
    return np.clip(matrix, -MAX_META_NUMERIC_ABS, MAX_META_NUMERIC_ABS)


def _sanitize_numeric_vector(values: np.ndarray) -> np.ndarray:
    vector = np.asarray(values, dtype=np.float64)
    vector = np.nan_to_num(
        vector,
        nan=0.0,
        posinf=MAX_META_NUMERIC_ABS,
        neginf=-MAX_META_NUMERIC_ABS,
    )
    return np.clip(vector, -MAX_META_NUMERIC_ABS, MAX_META_NUMERIC_ABS)


def normalized_model_config(model_type: str, config: Mapping[str, Any] | None = None) -> dict[str, Any]:
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
            "early_stopping_rounds": int(source.get("early_stopping_rounds", XGBOOST_EARLY_STOPPING_ROUNDS) or XGBOOST_EARLY_STOPPING_ROUNDS),
        }
    raise ValueError(f"Unsupported meta-selector model: {model_type}")


def build_model(model_type: str, config: Mapping[str, Any] | None = None):
    normalized = normalized_model_config(model_type, config)
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


def _fit_with_optional_early_stopping(
    model,
    *,
    model_type: str,
    config: Mapping[str, Any] | None = None,
    x_train: np.ndarray,
    y_train: np.ndarray,
    x_validation: np.ndarray | None = None,
    y_validation: np.ndarray | None = None,
):
    if model_type != "xgboost" or x_validation is None or y_validation is None or len(x_validation) == 0:
        model.fit(x_train, y_train)
        return model
    try:
        early_stopping_rounds = int((config or {}).get("early_stopping_rounds", XGBOOST_EARLY_STOPPING_ROUNDS) or XGBOOST_EARLY_STOPPING_ROUNDS)
        model.set_params(early_stopping_rounds=early_stopping_rounds)
    except (AttributeError, ValueError):
        pass
    fit_kwargs = {
        "eval_set": [(x_validation, y_validation)],
        "verbose": False,
    }
    try:
        model.fit(x_train, y_train, **fit_kwargs)
    except TypeError:
        fit_kwargs.pop("verbose", None)
        model.fit(x_train, y_train, **fit_kwargs)
    return model


def _xgboost_best_n_estimators(model, fallback_n_estimators: int) -> int:
    fallback = max(1, int(fallback_n_estimators))
    for attribute_name, offset in (("best_iteration", 1), ("best_iteration_", 1), ("best_ntree_limit", 0), ("best_ntree_limit_", 0)):
        raw_value = getattr(model, attribute_name, None)
        if raw_value is None:
            continue
        try:
            resolved = int(raw_value) + offset
        except (TypeError, ValueError):
            continue
        return max(1, min(fallback, resolved))
    return fallback


def _regression_quality(prefix: str, y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float | None]:
    if len(y_true) == 0:
        return {}
    mse = float(mean_squared_error(y_true, y_pred))
    return {
        f"{prefix}_mae": float(mean_absolute_error(y_true, y_pred)),
        f"{prefix}_mse": mse,
        f"{prefix}_rmse": float(np.sqrt(max(0.0, mse))),
        f"{prefix}_r2": float(r2_score(y_true, y_pred)) if len(y_true) > 1 else None,
    }


def window_key_expr() -> pl.Expr:
    return pl.concat_str([
        pl.col("test_started_at").dt.strftime("%Y-%m-%dT%H:%M:%S"),
        pl.lit("__"),
        pl.col("test_ended_at").dt.strftime("%Y-%m-%dT%H:%M:%S"),
    ]).alias("window_key")


def with_window_key(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    result = with_time_columns(frame)
    if "window_key" in result.columns:
        return result
    if "test_started_at" in result.columns and "test_ended_at" in result.columns:
        return result.with_columns(window_key_expr())
    return result


def objective_score_columns(frame: pl.DataFrame) -> tuple[str, str]:
    train_score_column = "train_objective_score" if "train_objective_score" in frame.columns else "train_score_log_trades"
    test_score_column = "test_objective_score" if "test_objective_score" in frame.columns else "test_score_log_trades"
    return train_score_column, test_score_column


def validation_split(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    keyed = with_window_key(frame)
    if keyed.is_empty() or "window_key" not in keyed.columns:
        return keyed, pl.DataFrame(schema=keyed.schema)
    windows = keyed.select(["window_key", "test_started_at"]).unique(maintain_order=True).sort("test_started_at")
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


def fit_predict(
    training_frame: pl.DataFrame,
    scoring_frame: pl.DataFrame,
    *,
    model_type: str,
    target_metric: str,
    model_config: Mapping[str, Any] | None = None,
) -> tuple[np.ndarray, int, int, float | None, float | None, dict[str, Any]]:
    training = with_engineered_columns(training_frame)
    scoring = with_engineered_columns(scoring_frame)
    normalized_config = normalized_model_config(model_type, model_config)
    if target_metric not in training.columns:
        raise ValueError(f"Target metric '{target_metric}' is not available in WFA history.")
    if training.height < 4:
        raise ValueError("Meta Selector needs at least 4 pre-OOS optimization rows.")
    x_score = (
        _sanitize_numeric_matrix(scoring.select(FEATURE_COLUMNS).to_numpy())
        if not scoring.is_empty()
        else np.empty((0, len(FEATURE_COLUMNS)))
    )

    split_train, split_validation = validation_split(training)
    validation_mae = None
    validation_r2 = None
    train_rows = training.height
    validation_rows = 0
    final_model_config = dict(normalized_config)
    quality_metrics: dict[str, Any] = {}
    if not split_validation.is_empty() and split_train.height >= 3:
        model = build_model(model_type, normalized_config)
        x_train = _sanitize_numeric_matrix(split_train.select(FEATURE_COLUMNS).to_numpy())
        y_train = _sanitize_numeric_vector(split_train.get_column(target_metric).to_numpy())
        x_validation = _sanitize_numeric_matrix(split_validation.select(FEATURE_COLUMNS).to_numpy())
        y_validation = _sanitize_numeric_vector(split_validation.get_column(target_metric).to_numpy())
        model = _fit_with_optional_early_stopping(
            model,
            model_type=model_type,
            config=normalized_config,
            x_train=x_train,
            y_train=y_train,
            x_validation=x_validation,
            y_validation=y_validation,
        )
        train_pred = model.predict(x_train)
        validation_pred = model.predict(x_validation)
        quality_metrics.update(_regression_quality("split_train", y_train, train_pred))
        quality_metrics.update(_regression_quality("validation", y_validation, validation_pred))
        validation_mae = float(mean_absolute_error(y_validation, validation_pred))
        validation_r2 = float(r2_score(y_validation, validation_pred)) if len(y_validation) > 1 else None
        train_rows = split_train.height
        validation_rows = split_validation.height
        if model_type == "xgboost":
            best_n_estimators = _xgboost_best_n_estimators(
                model,
                int(final_model_config.get("n_estimators", 1) or 1),
            )
            final_model_config["n_estimators"] = best_n_estimators
            quality_metrics.update(
                {
                    "xgboost_early_stopping_rounds": int(normalized_config.get("early_stopping_rounds", XGBOOST_EARLY_STOPPING_ROUNDS) or XGBOOST_EARLY_STOPPING_ROUNDS),
                    "xgboost_best_iteration": getattr(model, "best_iteration", getattr(model, "best_iteration_", None)),
                    "xgboost_best_n_estimators": int(best_n_estimators),
                }
            )

    final_model = build_model(model_type, final_model_config)
    x_training = _sanitize_numeric_matrix(training.select(FEATURE_COLUMNS).to_numpy())
    y_training = _sanitize_numeric_vector(training.get_column(target_metric).to_numpy())
    final_model.fit(x_training, y_training)
    training_pred = final_model.predict(x_training)
    quality_metrics.update(_regression_quality("train", y_training, training_pred))
    if model_type == "xgboost":
        quality_metrics.setdefault(
            "xgboost_early_stopping_rounds",
            int(normalized_config.get("early_stopping_rounds", XGBOOST_EARLY_STOPPING_ROUNDS) or XGBOOST_EARLY_STOPPING_ROUNDS),
        )
        quality_metrics["xgboost_final_n_estimators"] = int(final_model_config.get("n_estimators", normalized_config.get("n_estimators", 1)) or 1)
    predictions = np.asarray(final_model.predict(x_score), dtype=np.float64) if scoring.height else np.asarray([], dtype=np.float64)
    return predictions, train_rows, validation_rows, validation_mae, validation_r2, quality_metrics


def rank_parameter_sets(frame: pl.DataFrame, predictions: np.ndarray) -> list[dict[str, Any]]:
    enriched = with_window_key(with_engineered_columns(frame)).with_columns(pl.Series("predicted_target", predictions))
    train_score_column, test_score_column = objective_score_columns(enriched)
    grouped = (
        enriched.group_by(PARAMETER_COLUMNS)
        .agg(
            pl.len().alias("rows"),
            pl.col("fold").n_unique().alias("folds"),
            pl.col("predicted_target").mean().alias("predicted_mean"),
            pl.col("predicted_target").std(ddof=0).fill_null(0.0).alias("predicted_std"),
            pl.col(test_score_column).mean().alias("actual_test_score_mean"),
            pl.col("test_net_profit").mean().alias("actual_test_net_mean"),
            pl.col("test_max_drawdown").mean().alias("actual_test_maxdd_mean"),
            pl.col("test_cagr").mean().alias("actual_test_cagr_mean"),
            pl.col("test_cagr_to_ulcer").mean().alias("actual_test_cagr_to_ulcer_mean"),
            pl.col("test_r_squared").mean().alias("actual_test_r_squared_mean"),
            pl.col("test_hurst_exponent").mean().alias("actual_test_hurst_mean"),
            pl.col("test_calmar").mean().alias("actual_test_calmar_mean"),
            pl.col("test_trades").mean().alias("actual_test_trades_mean"),
            pl.col(train_score_column).mean().alias("train_score_mean"),
            pl.col("train_net_profit").mean().alias("train_net_mean"),
            pl.col("train_cagr").mean().alias("train_cagr_mean"),
            pl.col("train_cagr_to_ulcer").mean().alias("train_cagr_to_ulcer_mean"),
            pl.col("train_r_squared").mean().alias("train_r_squared_mean"),
            pl.col("train_hurst_exponent").mean().alias("train_hurst_mean"),
            pl.col("train_calmar").mean().alias("train_calmar_mean"),
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
                "actual_test_cagr_mean": float(row.get("actual_test_cagr_mean", 0.0) or 0.0),
                "actual_test_cagr_to_ulcer_mean": float(row.get("actual_test_cagr_to_ulcer_mean", 0.0) or 0.0),
                "actual_test_r_squared_mean": float(row.get("actual_test_r_squared_mean", 0.0) or 0.0),
                "actual_test_hurst_mean": float(row.get("actual_test_hurst_mean", 0.0) or 0.0),
                "actual_test_calmar_mean": float(row.get("actual_test_calmar_mean", 0.0) or 0.0),
                "actual_test_trades_mean": float(row.get("actual_test_trades_mean", 0.0) or 0.0),
                "train_score_mean": float(row.get("train_score_mean", 0.0) or 0.0),
                "train_net_mean": float(row.get("train_net_mean", 0.0) or 0.0),
                "train_cagr_mean": float(row.get("train_cagr_mean", 0.0) or 0.0),
                "train_cagr_to_ulcer_mean": float(row.get("train_cagr_to_ulcer_mean", 0.0) or 0.0),
                "train_r_squared_mean": float(row.get("train_r_squared_mean", 0.0) or 0.0),
                "train_hurst_mean": float(row.get("train_hurst_mean", 0.0) or 0.0),
                "train_calmar_mean": float(row.get("train_calmar_mean", 0.0) or 0.0),
            }
        )
    return rows


def select_rows_per_fold(frame: pl.DataFrame, predictions: np.ndarray) -> pl.DataFrame:
    enriched = with_window_key(with_engineered_columns(frame)).with_columns(pl.Series("predicted_target", predictions))
    if enriched.is_empty():
        return enriched
    train_score_column, _test_score_column = objective_score_columns(enriched)
    return (
        enriched
        .sort(
            ["test_started_at", "predicted_target", train_score_column, "train_net_profit", "train_pnl_to_maxdd", "trial_id"],
            descending=[False, True, True, True, True, False],
        )
        .group_by("window_key", maintain_order=True)
        .first()
        .sort("test_started_at")
    )
