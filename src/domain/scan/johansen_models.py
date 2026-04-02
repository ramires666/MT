from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from domain.contracts import PairSelection
from domain.scan.unit_root import PairUnitRootScreenResult


@dataclass(slots=True)
class JohansenScanParameters:
    det_order: int = 0
    k_ar_diff: int = 1
    significance_level: float = 0.05
    use_log_prices: bool = True
    zscore_lookback_bars: int = 96


@dataclass(slots=True)
class JohansenPairScanResult:
    pair: PairSelection
    sample_size: int
    transformed_mode: str
    unit_root: PairUnitRootScreenResult
    eligible_for_cointegration: bool
    rank: int = 0
    trace_statistics: tuple[float, ...] = ()
    max_eigen_statistics: tuple[float, ...] = ()
    trace_critical_values: tuple[tuple[float, ...], ...] = ()
    max_eigen_critical_values: tuple[tuple[float, ...], ...] = ()
    significance_column: str = "95%"
    threshold_passed: bool = False
    hedge_ratio: float | None = None
    eigenvector: tuple[float, ...] | None = None
    half_life_bars: float | None = None
    last_zscore: float | None = None
    failure_reason: str | None = None


@dataclass(slots=True)
class JohansenUniverseScanRow:
    symbol_1: str
    symbol_2: str
    sample_size: int
    eligible_for_cointegration: bool
    unit_root_leg_1: str
    unit_root_leg_2: str
    rank: int
    threshold_passed: bool
    trace_stat_0: float | None
    max_eigen_stat_0: float | None
    hedge_ratio: float | None
    half_life_bars: float | None
    last_zscore: float | None
    failure_reason: str | None


@dataclass(slots=True)
class JohansenUniverseScanSummary:
    total_symbols_requested: int
    loaded_symbols: int
    prefiltered_i1_symbols: int
    total_pairs_evaluated: int
    threshold_passed_pairs: int
    screened_out_pairs: int


@dataclass(slots=True)
class JohansenUniverseScanResult:
    summary: JohansenUniverseScanSummary
    rows: list[JohansenUniverseScanRow]
    universe_symbols: list[str]
    cancelled: bool = False


ProgressCallback = Callable[[int, int, str], None]
PartialResultCallback = Callable[[JohansenUniverseScanResult], None]
CancellationCheck = Callable[[], bool]
