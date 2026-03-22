from __future__ import annotations

from dataclasses import dataclass
from typing import Any


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
    source_wfa_run_id: str | None
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
            "source_wfa_run_id": self.source_wfa_run_id,
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
