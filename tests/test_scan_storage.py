from __future__ import annotations

import json
from datetime import UTC, datetime

import polars as pl

from domain.contracts import ScanUniverseMode, Timeframe
from domain.scan.johansen import JohansenUniverseScanResult, JohansenUniverseScanRow, JohansenUniverseScanSummary
from storage.scan_results import persist_johansen_scan_result


def test_persist_johansen_scan_result_handles_mixed_failure_reason(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv('MT_SERVICE_DATA_ROOT', str(tmp_path))

    result = JohansenUniverseScanResult(
        summary=JohansenUniverseScanSummary(
            total_symbols_requested=3,
            loaded_symbols=3,
            prefiltered_i1_symbols=2,
            total_pairs_evaluated=2,
            threshold_passed_pairs=1,
            screened_out_pairs=1,
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
                symbol_1='AUDCAD+',
                symbol_2='AUDCHF+',
                sample_size=95,
                eligible_for_cointegration=False,
                unit_root_leg_1='I(1)',
                unit_root_leg_2='I(1)',
                rank=0,
                threshold_passed=False,
                trace_stat_0=None,
                max_eigen_stat_0=None,
                hedge_ratio=None,
                half_life_bars=None,
                last_zscore=None,
                failure_reason='unit_root_gate_failed',
            ),
        ],
        universe_symbols=['US2000', 'NAS100', 'AUDCAD+', 'AUDCHF+'],
    )

    paths = persist_johansen_scan_result(
        broker='bybit_mt5',
        timeframe=Timeframe.M15,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ended_at=datetime(2026, 3, 17, tzinfo=UTC),
        universe_mode=ScanUniverseMode.GROUP,
        normalized_group='indices',
        symbols=None,
        result=result,
        unit_root_test_value='adf',
        det_order=0,
        k_ar_diff=1,
        significance_level=0.05,
        complete=False,
    )

    all_rows = pl.read_parquet(paths['all_pairs'])
    passed_rows = pl.read_parquet(paths['passed_pairs'])
    summary_payload = json.loads(paths['summary'].read_text(encoding='utf-8'))
    assert all_rows.height == 2
    assert passed_rows.height == 1
    assert all_rows.get_column('failure_reason').to_list()[1] == 'unit_root_gate_failed'
    assert summary_payload['complete'] is False
    assert summary_payload['scan_config'] == {
        'unit_root_test': 'adf',
        'det_order': 0,
        'k_ar_diff': 1,
        'significance_level': 0.05,
    }
