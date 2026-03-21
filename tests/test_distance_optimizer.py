from datetime import UTC, datetime, timedelta

import polars as pl

from domain.contracts import PairSelection, StrategyDefaults
from domain.optimizer import (
    count_distance_parameter_grid,
    optimize_distance_genetic_frame,
    optimize_distance_grid_frame,
    parse_distance_genetic_config,
    parse_distance_search_space,
)


def _sample_pair_frame() -> pl.DataFrame:
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=5 * idx) for idx in range(16)]
    close_1 = [100.0, 100.0, 100.0, 110.0, 112.0, 108.0, 102.0, 100.0, 99.0, 100.0, 104.0, 106.0, 101.0, 99.5, 100.0, 100.0]
    close_2 = [100.0] * 16
    return pl.DataFrame(
        {
            'time': times,
            'open_1': close_1,
            'high_1': close_1,
            'low_1': close_1,
            'close_1': close_1,
            'tick_volume_1': [100] * 16,
            'spread_1': [2] * 16,
            'real_volume_1': [10] * 16,
            'open_2': close_2,
            'high_2': close_2,
            'low_2': close_2,
            'close_2': close_2,
            'tick_volume_2': [100] * 16,
            'spread_2': [2] * 16,
            'real_volume_2': [10] * 16,
        }
    )


def test_parse_distance_search_space_supports_range_dicts() -> None:
    search_space = parse_distance_search_space(
        {
            'lookback_bars': {'start': 24, 'stop': 48, 'step': 24},
            'entry_z': {'start': 1.5, 'stop': 2.0, 'step': 0.5},
            'exit_z': {'start': 0.3, 'stop': 0.5, 'step': 0.2},
            'stop_z': {'start': 3.0, 'stop': 3.5, 'step': 0.5},
            'bollinger_k': {'start': 2.0, 'stop': 2.5, 'step': 0.5},
        }
    )

    assert search_space.lookback_bars == (24, 48)
    assert search_space.entry_z == (1.5, 2.0)
    assert search_space.exit_z == (0.3, 0.5)
    assert search_space.stop_z == (3.0, 3.5)
    assert search_space.bollinger_k == (2.0, 2.5)


def test_parse_distance_search_space_supports_disabled_stop() -> None:
    search_space = parse_distance_search_space(
        {
            'lookback_bars': [24, 48],
            'entry_z': [1.5, 2.0],
            'exit_z': [0.3],
            'stop_z': [None],
            'bollinger_k': [2.0],
        }
    )

    assert search_space.stop_z == (None,)
    assert count_distance_parameter_grid(search_space) == 4


def test_distance_grid_search_frame_returns_sorted_rows() -> None:
    result = optimize_distance_grid_frame(
        frame=_sample_pair_frame(),
        pair=PairSelection(symbol_1='US2000', symbol_2='NAS100'),
        defaults=StrategyDefaults(),
        search_space=parse_distance_search_space(
            {
                'lookback_bars': [3, 4],
                'entry_z': [1.0, 1.5],
                'exit_z': [0.2],
                'stop_z': [3.0],
                'bollinger_k': [2.0],
            }
        ),
        objective_metric='net_profit',
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
    )

    assert result.evaluated_trials == 4
    assert len(result.rows) == 4
    objective_scores = [row.objective_score for row in result.rows]
    assert objective_scores == sorted(objective_scores, reverse=True)
    assert {row.lookback_bars for row in result.rows} == {3, 4}
    assert {row.entry_z for row in result.rows} == {1.0, 1.5}


def test_distance_grid_search_frame_supports_disabled_stop() -> None:
    result = optimize_distance_grid_frame(
        frame=_sample_pair_frame(),
        pair=PairSelection(symbol_1='US2000', symbol_2='NAS100'),
        defaults=StrategyDefaults(),
        search_space=parse_distance_search_space(
            {
                'lookback_bars': [3, 4],
                'entry_z': [1.0, 1.5],
                'exit_z': [0.2],
                'stop_z': [None],
                'bollinger_k': [2.0],
            }
        ),
        objective_metric='net_profit',
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
    )

    assert result.evaluated_trials == 4
    assert result.rows
    assert {row.stop_z for row in result.rows} == {None}


def test_parse_distance_genetic_config_validates_defaults() -> None:
    config = parse_distance_genetic_config({'population_size': 10, 'generations': 4, 'elite_count': 2, 'mutation_rate': 0.15, 'random_seed': 7})

    assert config.population_size == 10
    assert config.generations == 4
    assert config.elite_count == 2
    assert config.mutation_rate == 0.15
    assert config.random_seed == 7


def test_distance_genetic_search_frame_returns_sorted_rows() -> None:
    result = optimize_distance_genetic_frame(
        frame=_sample_pair_frame(),
        pair=PairSelection(symbol_1='US2000', symbol_2='NAS100'),
        defaults=StrategyDefaults(),
        search_space=parse_distance_search_space(
            {
                'lookback_bars': [3, 4, 5],
                'entry_z': [1.0, 1.5, 2.0],
                'exit_z': [0.2, 0.5],
                'stop_z': [3.0, 3.5],
                'bollinger_k': [2.0],
            }
        ),
        objective_metric='net_profit',
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
        config=parse_distance_genetic_config(
            {
                'population_size': 8,
                'generations': 4,
                'elite_count': 2,
                'mutation_rate': 0.20,
                'random_seed': 11,
            }
        ),
    )

    assert result.evaluated_trials >= 1
    assert result.best_trial_id is not None
    assert result.rows
    objective_scores = [row.objective_score for row in result.rows]
    assert objective_scores == sorted(objective_scores, reverse=True)
    assert result.rows[0].trades >= 1
    assert result.rows[0].omega_ratio >= 0.0
