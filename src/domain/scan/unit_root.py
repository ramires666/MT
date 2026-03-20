from __future__ import annotations

from dataclasses import dataclass
import warnings

import numpy as np
from domain.contracts import UnitRootGate, UnitRootTest


@dataclass(slots=True)
class UnitRootScreenResult:
    test: UnitRootTest
    alpha: float
    sample_size: int
    difference_order: int
    passes_gate: bool
    inferred_order: str
    level_non_stationary: bool | None
    diff_stationary: bool | None
    failure_reason: str | None = None
    level_adf_pvalue: float | None = None
    diff_adf_pvalue: float | None = None
    level_kpss_pvalue: float | None = None
    diff_kpss_pvalue: float | None = None


@dataclass(slots=True)
class PairUnitRootScreenResult:
    leg_1: UnitRootScreenResult
    leg_2: UnitRootScreenResult
    eligible_for_cointegration: bool


def _clean_series(values: np.ndarray | list[float]) -> np.ndarray:
    array = np.asarray(values, dtype=np.float64)
    return array[np.isfinite(array)]


def _is_constant(values: np.ndarray) -> bool:
    if values.size == 0:
        return True
    return bool(np.allclose(values, values[0]))


def _difference(values: np.ndarray, order: int) -> np.ndarray:
    differenced = values
    for _ in range(order):
        differenced = np.diff(differenced)
    return differenced


def _run_adf(values: np.ndarray, gate: UnitRootGate) -> float | None:
    from statsmodels.tsa.stattools import adfuller

    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return float(adfuller(values, regression=gate.regression, autolag=gate.autolag)[1])
    except (ValueError, np.linalg.LinAlgError):
        return None


def _run_kpss(values: np.ndarray, gate: UnitRootGate) -> float | None:
    from statsmodels.tsa.stattools import kpss

    regression = gate.regression if gate.regression in {'c', 'ct'} else 'c'
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return float(kpss(values, regression=regression, nlags='auto')[1])
    except (ValueError, np.linalg.LinAlgError):
        return None


def _evaluate_gate(
    gate: UnitRootGate,
    *,
    level_adf_pvalue: float | None,
    diff_adf_pvalue: float | None,
    level_kpss_pvalue: float | None,
    diff_kpss_pvalue: float | None,
) -> tuple[bool, str, bool | None, bool | None, str | None]:
    level_non_stationary: bool | None = None
    diff_stationary: bool | None = None

    if gate.test is UnitRootTest.ADF:
        if level_adf_pvalue is None or diff_adf_pvalue is None:
            return False, 'unknown', None, None, 'unit_root_test_failed'
        level_non_stationary = level_adf_pvalue >= gate.alpha
        diff_stationary = diff_adf_pvalue < gate.alpha
    elif gate.test is UnitRootTest.KPSS:
        if level_kpss_pvalue is None or diff_kpss_pvalue is None:
            return False, 'unknown', None, None, 'unit_root_test_failed'
        level_non_stationary = level_kpss_pvalue < gate.alpha
        diff_stationary = diff_kpss_pvalue >= gate.alpha
    else:
        required = [level_adf_pvalue, diff_adf_pvalue, level_kpss_pvalue, diff_kpss_pvalue]
        if any(value is None for value in required):
            return False, 'unknown', None, None, 'unit_root_test_failed'
        level_non_stationary = level_adf_pvalue >= gate.alpha and level_kpss_pvalue < gate.alpha
        diff_stationary = diff_adf_pvalue < gate.alpha and diff_kpss_pvalue >= gate.alpha

    passes_gate = diff_stationary if not gate.require_i1 else level_non_stationary and diff_stationary
    if passes_gate:
        inferred_order = f'I({gate.difference_order})'
    elif level_non_stationary is False and diff_stationary is True:
        inferred_order = 'I(0)'
    elif level_non_stationary is True and diff_stationary is False:
        inferred_order = f'>I({gate.difference_order}) or unstable'
    else:
        inferred_order = 'unknown'
    return passes_gate, inferred_order, level_non_stationary, diff_stationary, None


def screen_series_for_cointegration(values: np.ndarray | list[float], gate: UnitRootGate) -> UnitRootScreenResult:
    clean = _clean_series(values)
    if clean.size < 32:
        return UnitRootScreenResult(
            test=gate.test,
            alpha=gate.alpha,
            sample_size=int(clean.size),
            difference_order=gate.difference_order,
            passes_gate=False,
            inferred_order='unknown',
            level_non_stationary=None,
            diff_stationary=None,
            failure_reason='insufficient_observations',
        )
    if _is_constant(clean):
        return UnitRootScreenResult(
            test=gate.test,
            alpha=gate.alpha,
            sample_size=int(clean.size),
            difference_order=gate.difference_order,
            passes_gate=False,
            inferred_order='unknown',
            level_non_stationary=None,
            diff_stationary=None,
            failure_reason='constant_series',
        )

    differenced = _difference(clean, gate.difference_order)
    if differenced.size < 16 or _is_constant(differenced):
        return UnitRootScreenResult(
            test=gate.test,
            alpha=gate.alpha,
            sample_size=int(clean.size),
            difference_order=gate.difference_order,
            passes_gate=False,
            inferred_order='unknown',
            level_non_stationary=None,
            diff_stationary=None,
            failure_reason='invalid_differenced_series',
        )

    level_adf_pvalue = _run_adf(clean, gate) if gate.test in {UnitRootTest.ADF, UnitRootTest.ADF_AND_KPSS} else None
    diff_adf_pvalue = _run_adf(differenced, gate) if gate.test in {UnitRootTest.ADF, UnitRootTest.ADF_AND_KPSS} else None
    level_kpss_pvalue = _run_kpss(clean, gate) if gate.test in {UnitRootTest.KPSS, UnitRootTest.ADF_AND_KPSS} else None
    diff_kpss_pvalue = _run_kpss(differenced, gate) if gate.test in {UnitRootTest.KPSS, UnitRootTest.ADF_AND_KPSS} else None

    passes_gate, inferred_order, level_non_stationary, diff_stationary, failure_reason = _evaluate_gate(
        gate,
        level_adf_pvalue=level_adf_pvalue,
        diff_adf_pvalue=diff_adf_pvalue,
        level_kpss_pvalue=level_kpss_pvalue,
        diff_kpss_pvalue=diff_kpss_pvalue,
    )
    return UnitRootScreenResult(
        test=gate.test,
        alpha=gate.alpha,
        sample_size=int(clean.size),
        difference_order=gate.difference_order,
        passes_gate=passes_gate,
        inferred_order=inferred_order,
        level_non_stationary=level_non_stationary,
        diff_stationary=diff_stationary,
        failure_reason=failure_reason,
        level_adf_pvalue=level_adf_pvalue,
        diff_adf_pvalue=diff_adf_pvalue,
        level_kpss_pvalue=level_kpss_pvalue,
        diff_kpss_pvalue=diff_kpss_pvalue,
    )


def screen_pair_for_cointegration(
    leg_1_values: np.ndarray | list[float],
    leg_2_values: np.ndarray | list[float],
    gate: UnitRootGate,
) -> PairUnitRootScreenResult:
    leg_1 = screen_series_for_cointegration(leg_1_values, gate)
    leg_2 = screen_series_for_cointegration(leg_2_values, gate)
    return PairUnitRootScreenResult(
        leg_1=leg_1,
        leg_2=leg_2,
        eligible_for_cointegration=leg_1.passes_gate and leg_2.passes_gate,
    )
