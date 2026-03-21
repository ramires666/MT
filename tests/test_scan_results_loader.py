from __future__ import annotations

import shutil
from datetime import UTC, datetime
from pathlib import Path

from app_config import get_settings
from domain.contracts import ScanUniverseMode, Timeframe
from domain.scan.johansen import JohansenUniverseScanResult, JohansenUniverseScanRow, JohansenUniverseScanSummary
from storage.scan_results import load_latest_saved_scan_result, partner_symbols_from_snapshot, persist_johansen_scan_result


TEST_ROOT = Path(r"W:\_python\MT\.tmp_scan_loader_test")


def test_load_latest_saved_scan_result_and_partner_filter(monkeypatch) -> None:
    if TEST_ROOT.exists():
        shutil.rmtree(TEST_ROOT)
    TEST_ROOT.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv('MT_SERVICE_DATA_ROOT', str(TEST_ROOT))
    get_settings.cache_clear()

    result = JohansenUniverseScanResult(
        summary=JohansenUniverseScanSummary(
            total_symbols_requested=3,
            loaded_symbols=3,
            prefiltered_i1_symbols=3,
            total_pairs_evaluated=2,
            threshold_passed_pairs=2,
            screened_out_pairs=0,
        ),
        rows=[
            JohansenUniverseScanRow(
                symbol_1='US2000',
                symbol_2='NAS100',
                sample_size=100,
                eligible_for_cointegration=True,
                unit_root_leg_1='I(1)',
                unit_root_leg_2='I(1)',
                rank=1,
                threshold_passed=True,
                trace_stat_0=12.5,
                max_eigen_stat_0=10.1,
                hedge_ratio=0.9,
                half_life_bars=22.0,
                last_zscore=0.8,
                failure_reason=None,
            ),
            JohansenUniverseScanRow(
                symbol_1='US2000',
                symbol_2='XAUUSD+',
                sample_size=95,
                eligible_for_cointegration=True,
                unit_root_leg_1='I(1)',
                unit_root_leg_2='I(1)',
                rank=1,
                threshold_passed=True,
                trace_stat_0=11.1,
                max_eigen_stat_0=9.5,
                hedge_ratio=1.1,
                half_life_bars=18.0,
                last_zscore=0.2,
                failure_reason=None,
            ),
        ],
        universe_symbols=['US2000', 'NAS100', 'XAUUSD+'],
    )

    persist_johansen_scan_result(
        broker='bybit_mt5',
        timeframe=Timeframe.M15,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ended_at=datetime(2026, 3, 17, tzinfo=UTC),
        universe_mode=ScanUniverseMode.GROUP,
        normalized_group='indices',
        symbols=None,
        result=result,
        created_at=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
    )

    snapshot = load_latest_saved_scan_result(
        broker='bybit_mt5',
        scan_kind='johansen',
        timeframe=Timeframe.M15,
        universe_mode=ScanUniverseMode.GROUP,
        normalized_group='indices',
    )

    assert snapshot is not None
    assert snapshot.scope == 'indices'
    assert snapshot.passed_pairs.height == 2
    assert partner_symbols_from_snapshot(snapshot, symbol_1='US2000', allowed_symbols=['US2000', 'NAS100', 'XAUUSD+']) == ['NAS100', 'XAUUSD+']

    get_settings.cache_clear()
