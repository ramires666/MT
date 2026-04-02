from datetime import UTC, datetime, timedelta

import numpy as np
import polars as pl

from domain.contracts import UnitRootGate, UnitRootTest
from domain.scan.johansen import JohansenScanParameters, JohansenUniverseScanResult, JohansenUniverseScanRow, JohansenUniverseScanSummary, scan_symbol_frames_johansen


def _time_index(size: int) -> list[datetime]:
    return [datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=5 * index) for index in range(size)]


def _frame(values: np.ndarray) -> pl.DataFrame:
    return pl.DataFrame({"time": _time_index(values.size), "close": values})


def _random_walk(seed: int, size: int = 800) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return np.cumsum(rng.normal(loc=0.0, scale=1.0, size=size)) + 200.0


def _stationary_ar1(seed: int, size: int = 800, phi: float = 0.35) -> np.ndarray:
    rng = np.random.default_rng(seed)
    noise = rng.normal(loc=0.0, scale=0.4, size=size)
    values = np.zeros(size, dtype=np.float64)
    values[0] = 50.0
    for index in range(1, size):
        values[index] = 50.0 + phi * (values[index - 1] - 50.0) + noise[index]
    return values


def test_universe_scan_prefilters_symbols_before_pair_scan() -> None:
    base = _random_walk(11)
    pair_leg = base + np.random.default_rng(21).normal(loc=0.0, scale=0.5, size=base.size)
    symbol_frames = {
        "AAA": _frame(base),
        "BBB": _frame(pair_leg),
        "CCC": _frame(_stationary_ar1(5)),
    }

    result = scan_symbol_frames_johansen(
        symbol_frames=symbol_frames,
        unit_root_gate=UnitRootGate(test=UnitRootTest.ADF),
        params=JohansenScanParameters(),
    )

    assert result.summary.total_symbols_requested == 3
    assert result.summary.loaded_symbols == 3
    assert result.summary.prefiltered_i1_symbols == 2
    assert result.summary.total_pairs_evaluated == 1
    assert len(result.rows) == 1
    assert result.rows[0].symbol_1 == 'AAA'
    assert result.rows[0].symbol_2 == 'BBB'


def test_universe_scan_resume_skips_already_processed_pairs(monkeypatch) -> None:
    base = _random_walk(11)
    symbol_frames = {
        "AAA": _frame(base),
        "BBB": _frame(base + 1.0),
        "CCC": _frame(base + 2.0),
    }

    class _PassGate:
        passes_gate = True

    scanned_pairs: list[tuple[str, str]] = []

    def fake_screen_symbol_payload(payload):
        return payload[0], _PassGate()

    def fake_scan_pair_payload(payload):
        symbol_1, symbol_2, *_rest = payload
        scanned_pairs.append((symbol_1, symbol_2))
        return JohansenUniverseScanRow(
            symbol_1=symbol_1,
            symbol_2=symbol_2,
            sample_size=100,
            eligible_for_cointegration=True,
            unit_root_leg_1='I(1)',
            unit_root_leg_2='I(1)',
            rank=1,
            threshold_passed=True,
            trace_stat_0=10.0,
            max_eigen_stat_0=9.0,
            hedge_ratio=1.0,
            half_life_bars=12.0,
            last_zscore=0.2,
            failure_reason=None,
        )

    monkeypatch.setattr('domain.scan.johansen_universe.screen_symbol_payload', fake_screen_symbol_payload)
    monkeypatch.setattr('domain.scan.johansen_universe.scan_pair_payload', fake_scan_pair_payload)

    resume_result = JohansenUniverseScanResult(
        summary=JohansenUniverseScanSummary(
            total_symbols_requested=3,
            loaded_symbols=3,
            prefiltered_i1_symbols=3,
            total_pairs_evaluated=1,
            threshold_passed_pairs=1,
            screened_out_pairs=0,
        ),
        rows=[
            JohansenUniverseScanRow(
                symbol_1='AAA',
                symbol_2='BBB',
                sample_size=100,
                eligible_for_cointegration=True,
                unit_root_leg_1='I(1)',
                unit_root_leg_2='I(1)',
                rank=1,
                threshold_passed=True,
                trace_stat_0=10.0,
                max_eigen_stat_0=9.0,
                hedge_ratio=1.0,
                half_life_bars=12.0,
                last_zscore=0.2,
                failure_reason=None,
            ),
        ],
        universe_symbols=['AAA', 'BBB', 'CCC'],
    )

    result = scan_symbol_frames_johansen(
        symbol_frames=symbol_frames,
        unit_root_gate=UnitRootGate(test=UnitRootTest.ADF),
        params=JohansenScanParameters(),
        parallel_workers=1,
        resume_result=resume_result,
    )

    assert scanned_pairs == [('AAA', 'CCC'), ('BBB', 'CCC')]
    assert result.summary.total_pairs_evaluated == 3


def test_universe_scan_cancel_returns_partial_rows(monkeypatch) -> None:
    base = _random_walk(11)
    symbol_frames = {
        "AAA": _frame(base),
        "BBB": _frame(base + 1.0),
        "CCC": _frame(base + 2.0),
    }

    class _PassGate:
        passes_gate = True

    partial_rows: list[int] = []
    cancel_counter = {"calls": 0}

    def fake_screen_symbol_payload(payload):
        return payload[0], _PassGate()

    def fake_scan_pair_payload(payload):
        symbol_1, symbol_2, *_rest = payload
        return JohansenUniverseScanRow(
            symbol_1=symbol_1,
            symbol_2=symbol_2,
            sample_size=100,
            eligible_for_cointegration=True,
            unit_root_leg_1='I(1)',
            unit_root_leg_2='I(1)',
            rank=1,
            threshold_passed=True,
            trace_stat_0=10.0,
            max_eigen_stat_0=9.0,
            hedge_ratio=1.0,
            half_life_bars=12.0,
            last_zscore=0.2,
            failure_reason=None,
        )

    def cancel_check() -> bool:
        cancel_counter["calls"] += 1
        return cancel_counter["calls"] > 4

    def partial_result_callback(result: JohansenUniverseScanResult) -> None:
        partial_rows.append(len(result.rows))

    monkeypatch.setattr('domain.scan.johansen_universe.screen_symbol_payload', fake_screen_symbol_payload)
    monkeypatch.setattr('domain.scan.johansen_universe.scan_pair_payload', fake_scan_pair_payload)

    result = scan_symbol_frames_johansen(
        symbol_frames=symbol_frames,
        unit_root_gate=UnitRootGate(test=UnitRootTest.ADF),
        params=JohansenScanParameters(),
        parallel_workers=1,
        partial_result_callback=partial_result_callback,
        cancel_check=cancel_check,
    )

    assert result.cancelled is True
    assert result.summary.total_pairs_evaluated == 1
    assert partial_rows
    assert partial_rows[-1] == 1


def test_universe_scan_respects_allowed_pair_keys(monkeypatch) -> None:
    base = _random_walk(11)
    symbol_frames = {
        "AAA": _frame(base),
        "BBB": _frame(base + 1.0),
        "CCC": _frame(base + 2.0),
    }

    class _PassGate:
        passes_gate = True

    scanned_pairs: list[tuple[str, str]] = []

    def fake_screen_symbol_payload(payload):
        return payload[0], _PassGate()

    def fake_scan_pair_payload(payload):
        symbol_1, symbol_2, *_rest = payload
        scanned_pairs.append((symbol_1, symbol_2))
        return JohansenUniverseScanRow(
            symbol_1=symbol_1,
            symbol_2=symbol_2,
            sample_size=100,
            eligible_for_cointegration=True,
            unit_root_leg_1='I(1)',
            unit_root_leg_2='I(1)',
            rank=1,
            threshold_passed=True,
            trace_stat_0=10.0,
            max_eigen_stat_0=9.0,
            hedge_ratio=1.0,
            half_life_bars=12.0,
            last_zscore=0.2,
            failure_reason=None,
        )

    monkeypatch.setattr('domain.scan.johansen_universe.screen_symbol_payload', fake_screen_symbol_payload)
    monkeypatch.setattr('domain.scan.johansen_universe.scan_pair_payload', fake_scan_pair_payload)

    result = scan_symbol_frames_johansen(
        symbol_frames=symbol_frames,
        unit_root_gate=UnitRootGate(test=UnitRootTest.ADF),
        params=JohansenScanParameters(),
        parallel_workers=1,
        allowed_pair_keys=["AAA::CCC"],
    )

    assert scanned_pairs == [('AAA', 'CCC')]
    assert result.summary.total_pairs_evaluated == 1
    assert [(row.symbol_1, row.symbol_2) for row in result.rows] == [('AAA', 'CCC')]
