import numpy as np

from domain.contracts import UnitRootGate, UnitRootTest
from domain.scan.unit_root import screen_pair_for_cointegration, screen_series_for_cointegration


def _random_walk(seed: int, size: int = 600) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return np.cumsum(rng.normal(loc=0.0, scale=1.0, size=size))


def _stationary_ar1(seed: int, size: int = 600, phi: float = 0.25) -> np.ndarray:
    rng = np.random.default_rng(seed)
    noise = rng.normal(loc=0.0, scale=1.0, size=size)
    values = np.zeros(size, dtype=np.float64)
    for index in range(1, size):
        values[index] = phi * values[index - 1] + noise[index]
    return values


def test_screen_series_adf_accepts_random_walk_as_i1() -> None:
    result = screen_series_for_cointegration(_random_walk(7), UnitRootGate(test=UnitRootTest.ADF))

    assert result.passes_gate is True
    assert result.inferred_order == 'I(1)'
    assert result.level_adf_pvalue is not None
    assert result.diff_adf_pvalue is not None


def test_screen_series_adf_rejects_stationary_series() -> None:
    result = screen_series_for_cointegration(_stationary_ar1(11), UnitRootGate(test=UnitRootTest.ADF))

    assert result.passes_gate is False
    assert result.level_non_stationary is False
    assert result.inferred_order == 'I(0)'


def test_screen_pair_requires_both_legs_to_pass() -> None:
    result = screen_pair_for_cointegration(
        _random_walk(3),
        _stationary_ar1(5),
        UnitRootGate(test=UnitRootTest.ADF),
    )

    assert result.leg_1.passes_gate is True
    assert result.leg_2.passes_gate is False
    assert result.eligible_for_cointegration is False
