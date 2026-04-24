from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class Algorithm(StrEnum):
    DISTANCE = "distance"
    OLS = "ols"
    JOHANSEN = "johansen"
    COPULA = "copula"


class OptimizationMode(StrEnum):
    GRID = "grid"
    GENETIC = "genetic"


class ScanUniverseMode(StrEnum):
    ALL = "all"
    MARKET_WATCH = "market_watch"
    GROUP = "group"
    MANUAL = "manual"


class Timeframe(StrEnum):
    M5 = "M5"
    M15 = "M15"
    M30 = "M30"
    H1 = "H1"
    H4 = "H4"
    D1 = "D1"


class NormalizedGroup(StrEnum):
    FOREX = "forex"
    INDICES = "indices"
    STOCKS = "stocks"
    COMMODITIES = "commodities"
    CRYPTO = "crypto"
    CUSTOM = "custom"


class WfaMode(StrEnum):
    ANCHORED = "anchored"
    ROLLING = "rolling"


class WfaPairMode(StrEnum):
    SINGLE = "single"
    MULTI = "multi"


class WfaSelectionSource(StrEnum):
    TESTER_MENU = "tester_menu"
    OPTIMIZATION_ROW = "optimization_row"


class WfaWindowUnit(StrEnum):
    WEEKS = "weeks"
    MONTHS = "months"
    BARS = "bars"


class UnitRootTest(StrEnum):
    ADF = "adf"
    KPSS = "kpss"
    ADF_AND_KPSS = "adf_and_kpss"


class StrategyDefaults(BaseModel):
    initial_capital: float = 10_000.0
    leverage: float = 100.0
    margin_budget_per_leg: float = 500.0
    slippage_points: float = 1.0


class PairSelection(BaseModel):
    symbol_1: str
    symbol_2: str
    normalized_group: NormalizedGroup | None = None
    only_cointegrated_pairs: bool = False
    source_scan_id: str | None = None


class IntegerRange(BaseModel):
    start: int
    stop: int
    step: int = 1


class WfaWindowSearchSpace(BaseModel):
    unit: WfaWindowUnit = WfaWindowUnit.WEEKS
    train: IntegerRange
    validation: IntegerRange
    test: IntegerRange
    walk_step: IntegerRange = Field(default_factory=lambda: IntegerRange(start=1, stop=1, step=1))
    train_unit: WfaWindowUnit | None = None
    validation_unit: WfaWindowUnit | None = None
    test_unit: WfaWindowUnit | None = None
    walk_step_unit: WfaWindowUnit | None = None

    def resolved_train_unit(self) -> WfaWindowUnit:
        return self.train_unit or self.unit

    def resolved_validation_unit(self) -> WfaWindowUnit:
        return self.validation_unit or self.unit

    def resolved_test_unit(self) -> WfaWindowUnit:
        return self.test_unit or self.unit

    def resolved_walk_step_unit(self) -> WfaWindowUnit:
        return self.walk_step_unit or self.unit


class UnitRootGate(BaseModel):
    test: UnitRootTest = UnitRootTest.ADF
    alpha: float = 0.05
    require_i1: bool = True
    difference_order: int = 1
    regression: str = "c"
    autolag: str = "AIC"


class BacktestRequest(BaseModel):
    pair: PairSelection
    algorithm: Algorithm
    timeframe: Timeframe
    started_at: datetime
    ended_at: datetime
    defaults: StrategyDefaults = Field(default_factory=StrategyDefaults)
    algorithm_params: dict[str, Any] = Field(default_factory=dict)


class OptimizationRequest(BaseModel):
    pair: PairSelection
    algorithm: Algorithm
    mode: OptimizationMode
    timeframe: Timeframe
    started_at: datetime
    ended_at: datetime
    objective_metric: str
    search_space: dict[str, Any] = Field(default_factory=dict)
    defaults: StrategyDefaults = Field(default_factory=StrategyDefaults)
    algorithm_params: dict[str, Any] = Field(default_factory=dict)


class WfaRequest(BaseModel):
    pairs: list[PairSelection]
    pair_mode: WfaPairMode
    selection_source: WfaSelectionSource
    algorithm: Algorithm
    timeframe: Timeframe
    started_at: datetime
    ended_at: datetime
    wfa_mode: WfaMode
    objective_metric: str
    window_search: WfaWindowSearchSpace
    defaults: StrategyDefaults = Field(default_factory=StrategyDefaults)
    algorithm_params: dict[str, Any] = Field(default_factory=dict)
    parameter_search_space: dict[str, Any] = Field(default_factory=dict)


class CointegrationScanRequest(BaseModel):
    algorithm: Algorithm = Algorithm.JOHANSEN
    timeframe: Timeframe = Timeframe.M15
    started_at: datetime
    ended_at: datetime
    universe_mode: ScanUniverseMode
    normalized_group: NormalizedGroup | None = None
    symbols: list[str] = Field(default_factory=list)
    unit_root_gate: UnitRootGate = Field(default_factory=UnitRootGate)

class JohansenPairScanRequest(BaseModel):
    pair: PairSelection
    timeframe: Timeframe = Timeframe.M15
    started_at: datetime
    ended_at: datetime
    unit_root_gate: UnitRootGate = Field(default_factory=UnitRootGate)
    det_order: int = 0
    k_ar_diff: int = 1
    significance_level: float = 0.05
    use_log_prices: bool = True
    zscore_lookback_bars: int = 96
