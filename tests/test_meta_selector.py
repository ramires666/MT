from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from domain.contracts import PairSelection, Timeframe
from domain.meta_selector import FEATURE_COLUMNS, _select_rows_per_fold, _validation_split, run_meta_selector


def test_run_meta_selector_decision_tree_ranks_saved_wfa_history(monkeypatch, tmp_path: Path) -> None:
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")
    history = pl.DataFrame(
        {
            "fold": [1, 1, 2, 2, 3, 3, 4, 4],
            "lookback_bars": [48, 96, 48, 96, 48, 96, 48, 96],
            "entry_z": [1.5, 2.0, 1.5, 2.0, 1.5, 2.0, 1.5, 2.0],
            "exit_z": [0.3, 0.5, 0.3, 0.5, 0.3, 0.5, 0.3, 0.5],
            "stop_enabled": [1, 1, 1, 1, 1, 1, 1, 1],
            "stop_z_value": [3.0, 3.5, 3.0, 3.5, 3.0, 3.5, 3.0, 3.5],
            "bollinger_k": [1.5, 2.0, 1.5, 2.0, 1.5, 2.0, 1.5, 2.0],
            "train_objective_score": [1.1, 0.6, 1.2, 0.5, 1.0, 0.4, 1.3, 0.3],
            "train_net_profit": [900, 200, 850, 180, 920, 210, 980, 160],
            "train_ending_equity": [10900, 10200, 10850, 10180, 10920, 10210, 10980, 10160],
            "train_max_drawdown": [220, 420, 240, 430, 210, 410, 230, 440],
            "train_pnl_to_maxdd": [4.1, 0.48, 3.5, 0.42, 4.38, 0.51, 4.26, 0.36],
            "train_omega_ratio": [1.8, 1.0, 1.7, 0.9, 1.9, 1.0, 2.0, 0.8],
            "train_k_ratio": [1.3, 0.4, 1.2, 0.3, 1.4, 0.5, 1.5, 0.2],
            "train_score_log_trades": [6.2, 1.2, 6.0, 1.1, 6.4, 1.3, 6.8, 1.0],
            "train_ulcer_index": [0.08, 0.22, 0.09, 0.24, 0.07, 0.21, 0.08, 0.25],
            "train_ulcer_performance": [1200, 120, 1180, 100, 1310, 140, 1250, 90],
            "train_trades": [18, 9, 17, 8, 19, 10, 20, 7],
            "train_win_rate": [0.68, 0.44, 0.66, 0.42, 0.70, 0.45, 0.72, 0.40],
            "train_gross_profit": [1200, 400, 1180, 360, 1260, 390, 1290, 340],
            "train_spread_cost": [120, 90, 130, 95, 125, 92, 128, 98],
            "train_slippage_cost": [20, 18, 20, 19, 21, 18, 22, 19],
            "train_commission_cost": [15, 12, 16, 13, 15, 12, 16, 13],
            "train_total_cost": [155, 120, 166, 127, 161, 122, 166, 130],
            "test_score_log_trades": [5.5, 0.8, 5.2, 0.7, 5.8, 0.9, 6.0, 0.6],
            "test_net_profit": [760, 90, 710, 70, 790, 110, 830, 50],
            "test_max_drawdown": [240, 470, 250, 480, 230, 450, 220, 500],
            "test_ending_equity": [10760, 10090, 10710, 10070, 10790, 10110, 10830, 10050],
            "test_pnl_to_maxdd": [3.16, 0.19, 2.84, 0.15, 3.43, 0.24, 3.77, 0.10],
            "test_omega_ratio": [1.6, 0.9, 1.5, 0.8, 1.7, 0.95, 1.8, 0.7],
            "test_k_ratio": [1.1, 0.2, 1.0, 0.1, 1.2, 0.3, 1.3, 0.1],
            "test_ulcer_index": [0.10, 0.28, 0.11, 0.29, 0.09, 0.27, 0.08, 0.31],
            "test_ulcer_performance": [980, 40, 930, 30, 1040, 50, 1100, 20],
            "test_trades": [12, 6, 11, 5, 13, 6, 14, 4],
            "test_win_rate": [0.64, 0.35, 0.62, 0.32, 0.66, 0.38, 0.68, 0.30],
            "test_total_cost": [110, 80, 112, 82, 108, 78, 106, 85],
        }
    )

    monkeypatch.setattr("domain.meta_selector.load_wfa_optimization_history", lambda broker, pair, timeframe: history)
    monkeypatch.setattr("domain.meta_selector.wfa_pair_history_path", lambda broker, pair, timeframe: tmp_path / "history.parquet")
    monkeypatch.setattr("domain.meta_selector.meta_selector_root", lambda: tmp_path / "meta")

    result = run_meta_selector(broker="bybit_mt5", pair=pair, timeframe=Timeframe.M15, model_type="decision_tree")

    assert result["failure_reason"] is None
    assert result["total_rows"] == history.height
    assert len(result["ranking_rows"]) >= 2
    assert result["ranking_rows"][0]["stability_score"] >= result["ranking_rows"][-1]["stability_score"]
    assert (tmp_path / "meta" / "bybit_mt5" / "M15" / "US2000__NAS100" / "decision_tree" / "ranking.parquet").exists()


def test_meta_selector_features_exclude_test_columns() -> None:
    assert all(not column.startswith("test_") for column in FEATURE_COLUMNS)


def test_select_rows_per_fold_uses_train_metrics_for_ties() -> None:
    frame = pl.DataFrame(
        {
            "fold": [1, 1],
            "trial_id": [10, 11],
            "test_started_at": [datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)],
            "test_ended_at": [datetime(2026, 1, 8, tzinfo=UTC), datetime(2026, 1, 8, tzinfo=UTC)],
            "lookback_bars": [48, 96],
            "entry_z": [1.5, 2.0],
            "exit_z": [0.3, 0.3],
            "stop_enabled": [1, 1],
            "stop_z_value": [3.0, 3.0],
            "bollinger_k": [1.5, 1.5],
            "train_objective_score": [1.0, 2.0],
            "train_net_profit": [100.0, 200.0],
            "train_ending_equity": [10100.0, 10200.0],
            "train_max_drawdown": [-50.0, -40.0],
            "train_pnl_to_maxdd": [2.0, 5.0],
            "train_omega_ratio": [1.1, 1.2],
            "train_k_ratio": [0.2, 0.3],
            "train_score_log_trades": [1.0, 3.0],
            "train_ulcer_index": [0.1, 0.1],
            "train_ulcer_performance": [10.0, 20.0],
            "train_trades": [5, 5],
            "train_win_rate": [0.5, 0.5],
            "train_gross_profit": [120.0, 220.0],
            "train_spread_cost": [10.0, 10.0],
            "train_slippage_cost": [1.0, 1.0],
            "train_commission_cost": [1.0, 1.0],
            "train_total_cost": [12.0, 12.0],
            "test_score_log_trades": [100.0, -100.0],
            "test_net_profit": [9999.0, -9999.0],
            "test_max_drawdown": [-10.0, -10.0],
            "test_ending_equity": [19999.0, 1.0],
            "test_pnl_to_maxdd": [99.0, -99.0],
            "test_omega_ratio": [2.0, 0.1],
            "test_k_ratio": [2.0, 0.1],
            "test_ulcer_index": [0.1, 0.9],
            "test_ulcer_performance": [100.0, -100.0],
            "test_trades": [5, 5],
            "test_win_rate": [1.0, 0.0],
            "test_total_cost": [5.0, 5.0],
            "test_commission_cost": [1.0, 1.0],
            "test_spread_cost": [1.0, 1.0],
            "test_slippage_cost": [1.0, 1.0],
        }
    )
    selected = _select_rows_per_fold(frame, predictions=[0.0, 0.0])
    assert selected.height == 1
    assert int(selected.get_column("trial_id")[0]) == 11


def test_validation_split_uses_windows_not_fold_numbers() -> None:
    rows = []
    for window_index, started_at in enumerate(
        [
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 8, tzinfo=UTC),
            datetime(2026, 1, 15, tzinfo=UTC),
            datetime(2026, 1, 22, tzinfo=UTC),
        ],
        start=1,
    ):
        ended_at = started_at.replace(day=started_at.day + 7)
        for trial_id in (1, 2, 3):
            rows.append(
                {
                    "fold": 1 if window_index % 2 else 2,
                    "trial_id": window_index * 10 + trial_id,
                    "test_started_at": started_at,
                    "test_ended_at": ended_at,
                    "lookback_bars": 48,
                    "entry_z": 1.5,
                    "exit_z": 0.3,
                    "stop_enabled": 1,
                    "stop_z_value": 3.0,
                    "bollinger_k": 1.5,
                    "train_objective_score": 1.0,
                    "train_net_profit": 100.0,
                    "train_ending_equity": 10100.0,
                    "train_max_drawdown": -50.0,
                    "train_pnl_to_maxdd": 2.0,
                    "train_omega_ratio": 1.1,
                    "train_k_ratio": 0.2,
                    "train_score_log_trades": 1.0,
                    "train_ulcer_index": 0.1,
                    "train_ulcer_performance": 10.0,
                    "train_trades": 5,
                    "train_win_rate": 0.5,
                    "train_gross_profit": 120.0,
                    "train_spread_cost": 10.0,
                    "train_slippage_cost": 1.0,
                    "train_commission_cost": 1.0,
                    "train_total_cost": 12.0,
                    "test_score_log_trades": 1.0,
                    "test_net_profit": 10.0,
                    "test_max_drawdown": -5.0,
                    "test_ending_equity": 10010.0,
                    "test_pnl_to_maxdd": 2.0,
                    "test_omega_ratio": 1.0,
                    "test_k_ratio": 0.2,
                    "test_ulcer_index": 0.1,
                    "test_ulcer_performance": 10.0,
                    "test_trades": 3,
                    "test_win_rate": 0.5,
                    "test_total_cost": 3.0,
                    "test_commission_cost": 1.0,
                    "test_spread_cost": 1.0,
                    "test_slippage_cost": 1.0,
                }
            )
    frame = pl.DataFrame(rows)
    training, validation = _validation_split(frame)
    assert training.height == 9
    assert validation.height == 3
    assert validation.get_column("test_started_at").n_unique() == 1
    assert training.get_column("test_started_at").n_unique() == 3
