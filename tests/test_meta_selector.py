from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import polars as pl

from domain.contracts import PairSelection, Timeframe
from domain.meta_selector import (
    FEATURE_COLUMNS,
    _latest_wfa_run_history,
    _select_rows_per_fold,
    _validation_split,
    load_saved_meta_selector_result,
    run_meta_selector,
)
from domain.meta_selector_ml import MAX_META_NUMERIC_ABS, fit_predict


def test_run_meta_selector_decision_tree_ranks_saved_wfa_history(monkeypatch, tmp_path: Path) -> None:
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")
    history = pl.DataFrame(
        {
            "fold": [1, 1, 2, 2, 3, 3, 4, 4],
            "trial_id": [11, 12, 21, 22, 31, 32, 41, 42],
            "test_started_at": [
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 8, tzinfo=UTC),
                datetime(2026, 1, 8, tzinfo=UTC),
                datetime(2026, 1, 15, tzinfo=UTC),
                datetime(2026, 1, 15, tzinfo=UTC),
                datetime(2026, 1, 22, tzinfo=UTC),
                datetime(2026, 1, 22, tzinfo=UTC),
            ],
            "test_ended_at": [
                datetime(2026, 1, 8, tzinfo=UTC),
                datetime(2026, 1, 8, tzinfo=UTC),
                datetime(2026, 1, 15, tzinfo=UTC),
                datetime(2026, 1, 15, tzinfo=UTC),
                datetime(2026, 1, 22, tzinfo=UTC),
                datetime(2026, 1, 22, tzinfo=UTC),
                datetime(2026, 1, 29, tzinfo=UTC),
                datetime(2026, 1, 29, tzinfo=UTC),
            ],
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
    monkeypatch.setattr(
        "domain.meta_selector.build_selected_fold_outputs",
        lambda **kwargs: (
            [{"fold": 1, "test_started_at": "2026-01-01T00:00:00Z"}],
            [{"time": "2026-01-01T00:00:00Z", "equity": 10_000.0}],
            0.0,
            0.0,
            0,
            0.0,
            0.0,
        ),
    )

    result = run_meta_selector(broker="bybit_mt5", pair=pair, timeframe=Timeframe.M15, model_type="decision_tree")

    assert result["failure_reason"] is None
    assert result["total_rows"] == history.height
    assert len(result["ranking_rows"]) >= 2
    assert result["ranking_rows"][0]["stability_score"] >= result["ranking_rows"][-1]["stability_score"]
    assert (tmp_path / "meta" / "bybit_mt5" / "M15" / "US2000__NAS100" / "decision_tree" / "ranking.parquet").exists()


def test_load_saved_meta_selector_result_reads_summary(monkeypatch, tmp_path: Path) -> None:
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")
    summary_dir = tmp_path / "meta" / "bybit_mt5" / "M15" / "US2000__NAS100" / "decision_tree"
    summary_dir.mkdir(parents=True, exist_ok=True)
    (summary_dir / "summary.json").write_text('{"status":"completed","selected_folds":[{"fold":1}],"ranking_rows":[{"rank":1}]}', encoding="utf-8")
    monkeypatch.setattr("domain.meta_selector.meta_selector_root", lambda: tmp_path / "meta")

    loaded = load_saved_meta_selector_result(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        model_type="decision_tree",
    )

    assert loaded is not None
    assert loaded["status"] == "completed"
    assert loaded["selected_folds"][0]["fold"] == 1


def test_meta_selector_features_exclude_test_columns() -> None:
    assert all(not column.startswith("test_") for column in FEATURE_COLUMNS)


def test_fit_predict_passes_new_metric_features_to_model(monkeypatch) -> None:
    feature_index = {name: idx for idx, name in enumerate(FEATURE_COLUMNS)}
    captured: dict[str, np.ndarray] = {}

    def make_row(*, marker: float) -> dict[str, float]:
        row = {column: 0.0 for column in FEATURE_COLUMNS}
        row["lookback_bars"] = 48.0 + marker
        row["entry_z"] = 1.5
        row["exit_z"] = 0.3
        row["stop_enabled"] = 1.0
        row["stop_z_value"] = 3.0
        row["bollinger_k"] = 2.0
        row["train_objective_score"] = 1.0 + marker
        row["train_cagr"] = 10.0 + marker
        row["train_cagr_to_ulcer"] = 20.0 + marker
        row["train_r_squared"] = 30.0 + marker
        row["train_calmar"] = 40.0 + marker
        row["train_beauty_score"] = 50.0 + marker
        row["test_score_log_trades"] = 0.5 + marker
        return row

    training = pl.DataFrame([make_row(marker=float(idx)) for idx in range(4)])
    scoring = pl.DataFrame([make_row(marker=99.0)])

    class FakeModel:
        def fit(self, x_train, y_train, **kwargs):
            captured["x_train"] = np.asarray(x_train, dtype=np.float64)
            captured["y_train"] = np.asarray(y_train, dtype=np.float64)
            return self

        def predict(self, x_score):
            matrix = np.asarray(x_score, dtype=np.float64)
            if "x_score" not in captured and matrix.shape[0] == scoring.height:
                captured["x_score"] = matrix
            return np.zeros(matrix.shape[0], dtype=np.float64)

    monkeypatch.setattr("domain.meta_selector_ml.build_model", lambda model_type, config=None: FakeModel())
    monkeypatch.setattr(
        "domain.meta_selector_ml.validation_split",
        lambda frame: (frame, pl.DataFrame(schema=frame.schema)),
    )

    fit_predict(
        training,
        scoring,
        model_type="decision_tree",
        target_metric="test_score_log_trades",
    )

    assert captured["x_train"][0, feature_index["train_cagr"]] == 10.0
    assert captured["x_train"][0, feature_index["train_cagr_to_ulcer"]] == 20.0
    assert captured["x_train"][0, feature_index["train_r_squared"]] == 30.0
    assert captured["x_train"][0, feature_index["train_calmar"]] == 40.0
    assert captured["x_train"][0, feature_index["train_beauty_score"]] == 50.0
    assert captured["x_score"][0, feature_index["train_cagr"]] == 109.0
    assert captured["x_score"][0, feature_index["train_cagr_to_ulcer"]] == 119.0
    assert captured["x_score"][0, feature_index["train_r_squared"]] == 129.0
    assert captured["x_score"][0, feature_index["train_calmar"]] == 139.0
    assert captured["x_score"][0, feature_index["train_beauty_score"]] == 149.0


def test_fit_predict_clips_extreme_feature_and_target_values(monkeypatch) -> None:
    feature_index = {name: idx for idx, name in enumerate(FEATURE_COLUMNS)}
    captured: dict[str, np.ndarray] = {}

    def make_row(*, train_cagr: float, test_cagr: float) -> dict[str, float]:
        row = {column: 0.0 for column in FEATURE_COLUMNS}
        row["lookback_bars"] = 48.0
        row["entry_z"] = 1.5
        row["exit_z"] = 0.3
        row["stop_enabled"] = 1.0
        row["stop_z_value"] = 3.0
        row["bollinger_k"] = 2.0
        row["train_cagr"] = train_cagr
        row["test_cagr"] = test_cagr
        return row

    training = pl.DataFrame(
        [
            make_row(train_cagr=float("inf"), test_cagr=float("inf")),
            make_row(train_cagr=-float("inf"), test_cagr=-float("inf")),
            make_row(train_cagr=1e300, test_cagr=1e300),
            make_row(train_cagr=-1e300, test_cagr=-1e300),
        ]
    )
    scoring = pl.DataFrame([make_row(train_cagr=float("inf"), test_cagr=0.0)])

    class FakeModel:
        def fit(self, x_train, y_train, **kwargs):
            captured["x_train"] = np.asarray(x_train, dtype=np.float64)
            captured["y_train"] = np.asarray(y_train, dtype=np.float64)
            return self

        def predict(self, x_score):
            matrix = np.asarray(x_score, dtype=np.float64)
            captured["x_score"] = matrix
            return np.zeros(matrix.shape[0], dtype=np.float64)

    monkeypatch.setattr("domain.meta_selector_ml.build_model", lambda model_type, config=None: FakeModel())
    monkeypatch.setattr(
        "domain.meta_selector_ml.validation_split",
        lambda frame: (frame, pl.DataFrame(schema=frame.schema)),
    )

    fit_predict(
        training,
        scoring,
        model_type="decision_tree",
        target_metric="test_cagr",
    )

    assert np.isfinite(captured["y_train"]).all()
    assert np.isfinite(captured["x_train"]).all()
    assert np.isfinite(captured["x_score"]).all()
    assert np.max(np.abs(captured["y_train"])) <= MAX_META_NUMERIC_ABS
    assert np.max(np.abs(captured["x_train"])) <= MAX_META_NUMERIC_ABS
    assert captured["y_train"][0] == MAX_META_NUMERIC_ABS
    assert captured["y_train"][1] == -MAX_META_NUMERIC_ABS
    assert captured["x_train"][0, feature_index["train_cagr"]] == MAX_META_NUMERIC_ABS
    assert captured["x_train"][1, feature_index["train_cagr"]] == -MAX_META_NUMERIC_ABS
    assert captured["x_score"][0, feature_index["train_cagr"]] == MAX_META_NUMERIC_ABS


def test_latest_wfa_run_history_prefers_latest_created_at() -> None:
    frame = pl.DataFrame(
        {
            "wfa_run_id": ["run_z", "run_z", "run_a", "run_a"],
            "created_at": [
                "2026-01-05T00:00:00+00:00",
                "2026-01-05T00:00:01+00:00",
                "2026-02-01T00:00:00+00:00",
                "2026-02-01T00:00:01+00:00",
            ],
            "fold": [1, 2, 1, 2],
        }
    )

    latest = _latest_wfa_run_history(frame)

    assert latest.height == 2
    assert set(latest.get_column("wfa_run_id").to_list()) == {"run_a"}


def test_run_meta_selector_uses_latest_wfa_run(monkeypatch, tmp_path: Path) -> None:
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")
    history = pl.DataFrame(
        {
            "wfa_run_id": ["run_old", "run_old", "run_new", "run_new"],
            "created_at": [
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
                "2026-02-01T00:00:00+00:00",
                "2026-02-01T00:00:01+00:00",
            ],
            "fold": [1, 2, 1, 2],
            "trial_id": [11, 12, 21, 22],
            "test_started_at": [
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 8, tzinfo=UTC),
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 8, tzinfo=UTC),
            ],
            "test_ended_at": [
                datetime(2026, 1, 8, tzinfo=UTC),
                datetime(2026, 1, 15, tzinfo=UTC),
                datetime(2026, 1, 8, tzinfo=UTC),
                datetime(2026, 1, 15, tzinfo=UTC),
            ],
            "lookback_bars": [48, 48, 96, 96],
            "entry_z": [1.5, 1.5, 2.0, 2.0],
            "exit_z": [0.3, 0.3, 0.5, 0.5],
            "stop_enabled": [1, 1, 1, 1],
            "stop_z_value": [3.0, 3.0, 4.0, 4.0],
            "bollinger_k": [1.5, 1.5, 2.0, 2.0],
            "train_score_log_trades": [1.0, 1.1, 4.0, 4.1],
            "train_net_profit": [100.0, 110.0, 400.0, 410.0],
            "train_pnl_to_maxdd": [2.0, 2.1, 6.0, 6.1],
            "train_trades": [5, 5, 10, 10],
            "test_trades": [3, 3, 6, 6],
            "test_score_log_trades": [0.2, 0.3, 0.8, 0.9],
        }
    )

    captured: dict[str, object] = {}

    def fake_fit_predict(training_history, oos_history, **_kwargs):
        captured["training_run_ids"] = sorted(set(training_history.get_column("wfa_run_id").to_list()))
        captured["oos_run_ids"] = sorted(set(oos_history.get_column("wfa_run_id").to_list()))
        return np.zeros(oos_history.height, dtype=np.float64), training_history.height, 0, None, None

    def fake_rank_parameter_sets(frame, _predictions):
        captured["ranking_run_ids"] = sorted(set(frame.get_column("wfa_run_id").to_list()))
        return [{"rank": 1, "rows": frame.height}]

    def fake_select_rows_per_fold(frame, _predictions):
        captured["selected_run_ids"] = sorted(set(frame.get_column("wfa_run_id").to_list()))
        return frame.head(1)

    monkeypatch.setattr("domain.meta_selector.load_wfa_optimization_history", lambda broker, pair, timeframe: history)
    monkeypatch.setattr("domain.meta_selector.wfa_pair_history_path", lambda broker, pair, timeframe: tmp_path / "history.parquet")
    monkeypatch.setattr("domain.meta_selector.meta_selector_root", lambda: tmp_path / "meta")
    monkeypatch.setattr("domain.meta_selector.with_engineered_columns", lambda frame: frame)
    monkeypatch.setattr("domain.meta_selector.fit_predict", fake_fit_predict)
    monkeypatch.setattr("domain.meta_selector.rank_parameter_sets", fake_rank_parameter_sets)
    monkeypatch.setattr("domain.meta_selector.select_rows_per_fold", fake_select_rows_per_fold)
    monkeypatch.setattr(
        "domain.meta_selector.build_selected_fold_outputs",
        lambda **kwargs: ([{"fold": 1, "test_started_at": "2026-01-01T00:00:00Z"}], [], 0.0, 0.0, 0, 0.0, 0.0),
    )
    monkeypatch.setattr("domain.meta_selector._persist_meta_selector_outputs", lambda **kwargs: tmp_path / "meta")

    result = run_meta_selector(broker="bybit_mt5", pair=pair, timeframe=Timeframe.M15, model_type="decision_tree")

    assert result["source_wfa_run_id"] == "run_new"
    assert result["total_rows"] == 2
    assert captured["training_run_ids"] == ["run_new"]
    assert captured["oos_run_ids"] == ["run_new"]
    assert captured["ranking_run_ids"] == ["run_new"]
    assert captured["selected_run_ids"] == ["run_new"]


def test_run_meta_selector_can_override_objective_metric(monkeypatch, tmp_path: Path) -> None:
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")
    history = pl.DataFrame(
        {
            "wfa_run_id": ["run_new", "run_new"],
            "created_at": [
                "2026-02-01T00:00:00+00:00",
                "2026-02-01T00:00:01+00:00",
            ],
            "objective_metric": ["score_log_trades", "score_log_trades"],
            "fold": [1, 2],
            "trial_id": [21, 22],
            "test_started_at": [
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 8, tzinfo=UTC),
            ],
            "test_ended_at": [
                datetime(2026, 1, 8, tzinfo=UTC),
                datetime(2026, 1, 15, tzinfo=UTC),
            ],
            "lookback_bars": [48, 96],
            "entry_z": [1.5, 2.0],
            "exit_z": [0.3, 0.5],
            "stop_enabled": [1, 1],
            "stop_z_value": [3.0, 4.0],
            "bollinger_k": [1.5, 2.0],
            "train_score_log_trades": [1.0, 1.1],
            "test_score_log_trades": [0.2, 0.3],
            "train_net_profit": [400.0, 410.0],
            "test_net_profit": [40.0, 41.0],
            "train_pnl_to_maxdd": [2.0, 2.1],
            "train_trades": [5, 5],
            "test_trades": [3, 3],
        }
    )

    captured: dict[str, object] = {}

    def fake_fit_predict(training_history, oos_history, **kwargs):
        captured["target_metric"] = kwargs["target_metric"]
        captured["train_objective_score"] = training_history.get_column("train_objective_score").to_list()
        captured["test_objective_score"] = oos_history.get_column("test_objective_score").to_list()
        return np.zeros(oos_history.height, dtype=np.float64), training_history.height, 0, None, None

    monkeypatch.setattr("domain.meta_selector.load_wfa_optimization_history", lambda broker, pair, timeframe: history)
    monkeypatch.setattr("domain.meta_selector.wfa_pair_history_path", lambda broker, pair, timeframe: tmp_path / "history.parquet")
    monkeypatch.setattr("domain.meta_selector.meta_selector_root", lambda: tmp_path / "meta")
    monkeypatch.setattr("domain.meta_selector.fit_predict", fake_fit_predict)
    monkeypatch.setattr("domain.meta_selector.rank_parameter_sets", lambda frame, _predictions: [{"rank": 1, "rows": frame.height}])
    monkeypatch.setattr("domain.meta_selector.select_rows_per_fold", lambda frame, _predictions: frame.head(1))
    monkeypatch.setattr(
        "domain.meta_selector.build_selected_fold_outputs",
        lambda **kwargs: ([{"fold": 1, "test_started_at": "2026-01-01T00:00:00Z"}], [], 0.0, 0.0, 0, 0.0, 0.0),
    )
    monkeypatch.setattr("domain.meta_selector._persist_meta_selector_outputs", lambda **kwargs: tmp_path / "meta")

    result = run_meta_selector(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        model_type="decision_tree",
        objective_metric="net_profit",
    )

    assert captured["target_metric"] == "test_objective_score"
    assert captured["train_objective_score"] == [400.0, 410.0]
    assert captured["test_objective_score"] == [40.0, 41.0]
    assert result["source_objective_metric"] == "score_log_trades"
    assert result["selected_objective_metric"] == "net_profit"


def test_run_meta_selector_trains_strictly_before_oos_start(monkeypatch, tmp_path: Path) -> None:
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")
    history = pl.DataFrame(
        {
            "wfa_run_id": ["run_new"] * 6,
            "created_at": ["2026-02-01T00:00:00+00:00"] * 6,
            "fold": [1, 1, 2, 2, 3, 3],
            "trial_id": [11, 12, 21, 22, 31, 32],
            "test_started_at": [
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 8, tzinfo=UTC),
                datetime(2026, 1, 8, tzinfo=UTC),
                datetime(2026, 1, 15, tzinfo=UTC),
                datetime(2026, 1, 15, tzinfo=UTC),
            ],
            "test_ended_at": [
                datetime(2026, 1, 8, tzinfo=UTC),
                datetime(2026, 1, 8, tzinfo=UTC),
                datetime(2026, 1, 15, tzinfo=UTC),
                datetime(2026, 1, 15, tzinfo=UTC),
                datetime(2026, 1, 22, tzinfo=UTC),
                datetime(2026, 1, 22, tzinfo=UTC),
            ],
            "lookback_bars": [48, 96, 48, 96, 48, 96],
            "entry_z": [1.5, 2.0, 1.5, 2.0, 1.5, 2.0],
            "exit_z": [0.3, 0.5, 0.3, 0.5, 0.3, 0.5],
            "stop_enabled": [1, 1, 1, 1, 1, 1],
            "stop_z_value": [3.0, 3.5, 3.0, 3.5, 3.0, 3.5],
            "bollinger_k": [1.5, 2.0, 1.5, 2.0, 1.5, 2.0],
            "train_score_log_trades": [1.0, 0.9, 1.1, 0.8, 1.2, 0.7],
            "train_net_profit": [100.0, 90.0, 110.0, 80.0, 120.0, 70.0],
            "train_pnl_to_maxdd": [2.0, 1.8, 2.1, 1.7, 2.2, 1.6],
            "train_trades": [5, 4, 5, 4, 5, 4],
            "test_score_log_trades": [0.4, 0.3, 0.5, 0.2, 0.6, 0.1],
            "test_net_profit": [40.0, 30.0, 50.0, 20.0, 60.0, 10.0],
            "test_pnl_to_maxdd": [1.5, 1.4, 1.6, 1.3, 1.7, 1.2],
            "test_trades": [3, 2, 3, 2, 3, 2],
        }
    )

    captured: dict[str, object] = {}

    def fake_fit_predict(training_history, oos_history, **_kwargs):
        captured["training_starts"] = training_history.get_column("test_started_at").unique().sort().to_list()
        captured["oos_starts"] = oos_history.get_column("test_started_at").unique().sort().to_list()
        return np.zeros(oos_history.height, dtype=np.float64), training_history.height, 0, None, None

    monkeypatch.setattr("domain.meta_selector.load_wfa_optimization_history", lambda broker, pair, timeframe: history)
    monkeypatch.setattr("domain.meta_selector.wfa_pair_history_path", lambda broker, pair, timeframe: tmp_path / "history.parquet")
    monkeypatch.setattr("domain.meta_selector.meta_selector_root", lambda: tmp_path / "meta")
    monkeypatch.setattr("domain.meta_selector.fit_predict", fake_fit_predict)
    monkeypatch.setattr("domain.meta_selector.rank_parameter_sets", lambda frame, _predictions: [{"rank": 1, "rows": frame.height}])
    monkeypatch.setattr("domain.meta_selector.select_rows_per_fold", lambda frame, _predictions: frame.head(1))
    monkeypatch.setattr(
        "domain.meta_selector.build_selected_fold_outputs",
        lambda **kwargs: ([{"fold": 2, "test_started_at": "2026-01-08T00:00:00Z"}], [], 0.0, 0.0, 0, 0.0, 0.0),
    )
    monkeypatch.setattr("domain.meta_selector._persist_meta_selector_outputs", lambda **kwargs: tmp_path / "meta")

    result = run_meta_selector(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        model_type="decision_tree",
        oos_started_at=datetime(2026, 1, 8, tzinfo=UTC),
    )

    assert result["failure_reason"] is None
    assert captured["training_starts"] == [datetime(2026, 1, 1, tzinfo=UTC)]
    assert captured["oos_starts"] == [
        datetime(2026, 1, 8, tzinfo=UTC),
        datetime(2026, 1, 15, tzinfo=UTC),
    ]


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


def test_xgboost_fit_predict_uses_early_stopping_and_best_iteration(monkeypatch) -> None:
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
                    "fold": window_index,
                    "trial_id": window_index * 10 + trial_id,
                    "test_started_at": started_at,
                    "test_ended_at": ended_at,
                    "lookback_bars": 48 + (trial_id * 2),
                    "entry_z": 1.5 + (trial_id * 0.1),
                    "exit_z": 0.3,
                    "stop_enabled": 1,
                    "stop_z_value": 3.0,
                    "bollinger_k": 1.5,
                    "train_objective_score": 1.0 + trial_id,
                    "train_net_profit": 100.0 + (window_index * 10) + trial_id,
                    "train_ending_equity": 10100.0 + (window_index * 10) + trial_id,
                    "train_max_drawdown": -50.0,
                    "train_pnl_to_maxdd": 2.0 + (trial_id * 0.1),
                    "train_omega_ratio": 1.1,
                    "train_k_ratio": 0.2,
                    "train_score_log_trades": 1.0 + trial_id,
                    "train_ulcer_index": 0.1,
                    "train_ulcer_performance": 10.0,
                    "train_trades": 5,
                    "train_win_rate": 0.5,
                    "train_gross_profit": 120.0,
                    "train_spread_cost": 10.0,
                    "train_slippage_cost": 1.0,
                    "train_commission_cost": 1.0,
                    "train_total_cost": 12.0,
                    "test_score_log_trades": 1.0 + (trial_id * 0.1),
                    "test_net_profit": 10.0 + trial_id,
                    "test_max_drawdown": -5.0,
                    "test_ending_equity": 10010.0 + trial_id,
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
    build_configs: list[dict[str, object]] = []
    models: list[object] = []

    class FakeXgbModel:
        def __init__(self, *, prediction_value: float, best_iteration: int | None = None) -> None:
            self.prediction_value = float(prediction_value)
            self.best_iteration = best_iteration
            self.params: dict[str, object] = {}
            self.fit_calls: list[dict[str, object]] = []

        def set_params(self, **kwargs):
            self.params.update(kwargs)
            return self

        def fit(self, x_train, y_train, **kwargs):
            self.fit_calls.append(
                {
                    "rows": int(len(x_train)),
                    "eval_rows": int(len(kwargs.get("eval_set", [((), ())])[0][0])) if kwargs.get("eval_set") else 0,
                    "verbose": kwargs.get("verbose"),
                }
            )
            return self

        def predict(self, x_score):
            return np.full(len(x_score), self.prediction_value, dtype=np.float64)

    def fake_build_model(model_type: str, config=None):
        assert model_type == "xgboost"
        config_dict = dict(config or {})
        build_configs.append(config_dict)
        if not models:
            model = FakeXgbModel(prediction_value=0.25, best_iteration=16)
        else:
            model = FakeXgbModel(prediction_value=0.5, best_iteration=None)
        models.append(model)
        return model

    monkeypatch.setattr("domain.meta_selector_ml.build_model", fake_build_model)

    predictions, train_rows, validation_rows, validation_mae, validation_r2, quality_metrics = fit_predict(
        frame,
        frame.head(2),
        model_type="xgboost",
        target_metric="test_score_log_trades",
        model_config={
            "n_estimators": 240,
            "max_depth": 4,
            "learning_rate": 0.05,
            "subsample": 0.9,
            "colsample_bytree": 0.9,
            "early_stopping_rounds": 17,
        },
    )

    assert train_rows == 9
    assert validation_rows == 3
    assert validation_mae is not None
    assert validation_r2 is not None
    assert quality_metrics["validation_mae"] is not None
    assert quality_metrics["validation_rmse"] is not None
    assert quality_metrics["validation_mse"] is not None
    assert quality_metrics["train_mae"] is not None
    assert quality_metrics["train_rmse"] is not None
    assert quality_metrics["train_mse"] is not None
    assert len(predictions) == 2
    assert np.allclose(predictions, np.asarray([0.5, 0.5], dtype=np.float64))
    assert models[0].params["early_stopping_rounds"] == 17
    assert models[0].fit_calls[0]["eval_rows"] == 3
    assert models[0].fit_calls[0]["verbose"] is False
    assert quality_metrics["xgboost_early_stopping_rounds"] == 17
    assert quality_metrics["xgboost_best_iteration"] == 16
    assert quality_metrics["xgboost_best_n_estimators"] == 17
    assert quality_metrics["xgboost_final_n_estimators"] == 17
    assert int(build_configs[1]["n_estimators"]) == 17
