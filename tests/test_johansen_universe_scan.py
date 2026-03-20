from datetime import UTC, datetime, timedelta

import numpy as np
import polars as pl

from domain.contracts import UnitRootGate, UnitRootTest
from domain.scan.johansen import JohansenScanParameters, scan_symbol_frames_johansen


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
