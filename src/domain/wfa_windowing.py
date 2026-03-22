from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime, timedelta

from domain.contracts import Timeframe, WfaMode, WfaWindowUnit


@dataclass(slots=True)
class WalkWindow:
    index: int
    train_started_at: datetime
    train_ended_at: datetime
    validation_started_at: datetime
    validation_ended_at: datetime
    test_started_at: datetime
    test_ended_at: datetime


BAR_MINUTES = {
    Timeframe.M5: 5,
    Timeframe.M15: 15,
    Timeframe.M30: 30,
    Timeframe.H1: 60,
    Timeframe.H4: 240,
    Timeframe.D1: 1440,
}


def add_months(moment: datetime, months: int) -> datetime:
    month_index = (moment.month - 1) + int(months)
    year = moment.year + month_index // 12
    month = (month_index % 12) + 1
    day = min(moment.day, monthrange(year, month)[1])
    return moment.replace(year=year, month=month, day=day)


def advance(moment: datetime, units: int, unit: WfaWindowUnit, timeframe: Timeframe) -> datetime:
    units = int(units)
    if unit == WfaWindowUnit.WEEKS:
        return moment + timedelta(weeks=units)
    if unit == WfaWindowUnit.MONTHS:
        return add_months(moment, units)
    if unit == WfaWindowUnit.BARS:
        return moment + timedelta(minutes=BAR_MINUTES[timeframe] * units)
    raise ValueError(f"Unsupported WFA window unit: {unit}")


def build_walk_windows(
    *,
    started_at: datetime,
    ended_at: datetime,
    mode: WfaMode,
    timeframe: Timeframe,
    train_units: int,
    validation_units: int,
    test_units: int,
    walk_step_units: int,
    train_unit: WfaWindowUnit,
    validation_unit: WfaWindowUnit,
    test_unit: WfaWindowUnit,
    walk_step_unit: WfaWindowUnit,
) -> list[WalkWindow]:
    if min(train_units, validation_units, test_units, walk_step_units) <= 0:
        return []

    windows: list[WalkWindow] = []
    if mode == WfaMode.ANCHORED:
        train_start = started_at
        train_end = advance(train_start, train_units, train_unit, timeframe)
        index = 0
        while True:
            validation_start = train_end
            validation_end = advance(validation_start, validation_units, validation_unit, timeframe)
            test_start = validation_end
            test_end = advance(test_start, test_units, test_unit, timeframe)
            if test_end > ended_at:
                break
            windows.append(
                WalkWindow(
                    index=index,
                    train_started_at=train_start,
                    train_ended_at=train_end,
                    validation_started_at=validation_start,
                    validation_ended_at=validation_end,
                    test_started_at=test_start,
                    test_ended_at=test_end,
                )
            )
            train_end = advance(train_end, walk_step_units, walk_step_unit, timeframe)
            index += 1
        return windows

    train_start = started_at
    index = 0
    while True:
        train_end = advance(train_start, train_units, train_unit, timeframe)
        validation_start = train_end
        validation_end = advance(validation_start, validation_units, validation_unit, timeframe)
        test_start = validation_end
        test_end = advance(test_start, test_units, test_unit, timeframe)
        if test_end > ended_at:
            break
        windows.append(
            WalkWindow(
                index=index,
                train_started_at=train_start,
                train_ended_at=train_end,
                validation_started_at=validation_start,
                validation_ended_at=validation_end,
                test_started_at=test_start,
                test_ended_at=test_end,
            )
        )
        train_start = advance(train_start, walk_step_units, walk_step_unit, timeframe)
        index += 1
    return windows


def build_train_test_windows(
    *,
    started_at: datetime,
    ended_at: datetime,
    timeframe: Timeframe,
    lookback_units: int,
    test_units: int,
    step_units: int,
    unit: WfaWindowUnit,
) -> list[WalkWindow]:
    if min(lookback_units, test_units, step_units) <= 0:
        return []
    windows: list[WalkWindow] = []
    train_started_at = started_at
    index = 0
    while True:
        train_ended_at = advance(train_started_at, lookback_units, unit, timeframe)
        test_started_at = train_ended_at
        test_ended_at = advance(test_started_at, test_units, unit, timeframe)
        if test_ended_at > ended_at:
            break
        windows.append(
            WalkWindow(
                index=index,
                train_started_at=train_started_at,
                train_ended_at=train_ended_at,
                validation_started_at=train_ended_at,
                validation_ended_at=train_ended_at,
                test_started_at=test_started_at,
                test_ended_at=test_ended_at,
            )
        )
        train_started_at = advance(train_started_at, step_units, unit, timeframe)
        index += 1
    return windows
