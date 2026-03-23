from bokeh_app.numeric_inputs import fractional_step_decimals, has_fractional_step, normalize_fractional_value


def test_fractional_step_decimals_uses_step_precision() -> None:
    assert fractional_step_decimals(0.1) == 1
    assert fractional_step_decimals(0.05) == 2
    assert fractional_step_decimals(0.01) == 2
    assert fractional_step_decimals(1.0) == 0


def test_has_fractional_step_detects_fractional_spinners() -> None:
    assert has_fractional_step(0.1) is True
    assert has_fractional_step(0.05) is True
    assert has_fractional_step(1.0) is False


def test_normalize_fractional_value_removes_binary_float_tails() -> None:
    assert normalize_fractional_value(-1.2999999999999998, step=0.1, low=-5.0, high=5.0) == -1.3
    assert normalize_fractional_value(0.1999999999999984, step=0.1, low=0.1, high=5.0) == 0.2


def test_normalize_fractional_value_clamps_and_clears_negative_zero() -> None:
    assert normalize_fractional_value(1.009, step=0.01, low=0.01, high=1.0) == 1.0
    assert normalize_fractional_value(-0.00000000001, step=0.1, low=-5.0, high=5.0) == 0.0
