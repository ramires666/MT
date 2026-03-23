import pytest
import numpy as np

from domain.contracts import PairSelection, UnitRootGate, UnitRootTest
from domain.scan.johansen import JohansenScanParameters, scan_pair_johansen_arrays


def _random_walk(seed: int, size: int = 800) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return np.cumsum(rng.normal(loc=0.0, scale=1.0, size=size))


def _stationary_ar1(seed: int, size: int = 800, phi: float = 0.35) -> np.ndarray:
    rng = np.random.default_rng(seed)
    noise = rng.normal(loc=0.0, scale=1.0, size=size)
    values = np.zeros(size, dtype=np.float64)
    for index in range(1, size):
        values[index] = phi * values[index - 1] + noise[index]
    return values


def test_johansen_scan_rejects_pair_that_fails_unit_root_gate() -> None:
    result = scan_pair_johansen_arrays(
        pair=PairSelection(symbol_1='RW1', symbol_2='AR1'),
        leg_1_values=_random_walk(1),
        leg_2_values=_stationary_ar1(2),
        unit_root_gate=UnitRootGate(test=UnitRootTest.ADF),
        params=JohansenScanParameters(),
    )

    assert result.eligible_for_cointegration is False
    assert result.failure_reason == 'unit_root_gate_failed'
    assert result.rank == 0


def test_johansen_scan_detects_cointegrated_pair() -> None:
    base = _random_walk(11) + 500.0
    rng = np.random.default_rng(21)
    pair_leg = base + rng.normal(loc=0.0, scale=0.5, size=base.size)

    result = scan_pair_johansen_arrays(
        pair=PairSelection(symbol_1='X', symbol_2='Y'),
        leg_1_values=base,
        leg_2_values=pair_leg,
        unit_root_gate=UnitRootGate(test=UnitRootTest.ADF),
        params=JohansenScanParameters(k_ar_diff=1, det_order=0, significance_level=0.05),
    )

    assert result.eligible_for_cointegration is True
    assert result.threshold_passed is True
    assert result.rank >= 1
    assert result.hedge_ratio is not None
    assert result.last_zscore is not None


def test_johansen_scan_rejects_unsupported_det_order() -> None:
    base = _random_walk(11) + 500.0
    rng = np.random.default_rng(21)
    pair_leg = base + rng.normal(loc=0.0, scale=0.5, size=base.size)

    with pytest.raises(ValueError, match=r"supports only -1, 0, 1"):
        scan_pair_johansen_arrays(
            pair=PairSelection(symbol_1='X', symbol_2='Y'),
            leg_1_values=base,
            leg_2_values=pair_leg,
            unit_root_gate=UnitRootGate(test=UnitRootTest.ADF),
            params=JohansenScanParameters(k_ar_diff=1, det_order=2, significance_level=0.05),
        )
