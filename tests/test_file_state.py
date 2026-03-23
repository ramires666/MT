from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from bokeh.models import Button, DateRangeSlider, Div, Select, Spinner

from bokeh_app.browser_state import BrowserStateBinding
import bokeh_app.file_state as file_state_module
from bokeh_app.file_state import FileStateController


def test_file_state_persists_and_restores_controls() -> None:
    state_path = Path('tests/.tmp_bokeh_file_state.json')
    if state_path.exists():
        state_path.unlink()
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC).replace(tzinfo=None)
        end = datetime(2026, 3, 1, tzinfo=UTC).replace(tzinfo=None)
        group = Select(value='forex', options=['all', 'forex', 'indices'])
        symbol = Select(value='AUDCAD+', options=['AUDCAD+', 'AUDCHF+'])
        period = DateRangeSlider(start=start, end=end, value=(start, end))
        lookback = Spinner(value=96, low=1, step=1)
        body = Div(visible=False)
        toggle = Button(button_type='default')
        bindings = [
            BrowserStateBinding('group', group, kind='select'),
            BrowserStateBinding('symbol_1', symbol, kind='select', restore_on_options_change=True),
            BrowserStateBinding('period', period, kind='range'),
            BrowserStateBinding('lookback', lookback),
            BrowserStateBinding('show_plot', body, property_name='visible', kind='visible'),
            BrowserStateBinding('show_plot_button', toggle, property_name='button_type', default='primary'),
        ]
        controller = FileStateController(state_path, bindings)
        group.value = 'indices'
        symbol.value = 'AUDCHF+'
        period.value = (datetime(2026, 1, 15), datetime(2026, 2, 20))
        lookback.value = 144
        body.visible = True
        toggle.button_type = 'primary'
        controller.persist()

        restored_group = Select(value='all', options=['all', 'forex', 'indices'])
        restored_symbol = Select(value='AUDCAD+', options=['AUDCAD+', 'AUDCHF+'])
        restored_period = DateRangeSlider(start=start, end=end, value=(start, end))
        restored_lookback = Spinner(value=48, low=1, step=1)
        restored_body = Div(visible=False)
        restored_toggle = Button(button_type='default')
        restored_bindings = [
            BrowserStateBinding('group', restored_group, kind='select'),
            BrowserStateBinding('symbol_1', restored_symbol, kind='select', restore_on_options_change=True),
            BrowserStateBinding('period', restored_period, kind='range'),
            BrowserStateBinding('lookback', restored_lookback),
            BrowserStateBinding('show_plot', restored_body, property_name='visible', kind='visible'),
            BrowserStateBinding('show_plot_button', restored_toggle, property_name='button_type', default='primary'),
        ]
        restored_controller = FileStateController(state_path, restored_bindings)
        restored_controller.restore()

        assert restored_group.value == 'indices'
        assert restored_symbol.value == 'AUDCHF+'
        assert restored_lookback.value == 144
        assert restored_body.visible is True
        assert restored_toggle.button_type == 'primary'
        start_restored = datetime.fromtimestamp(float(restored_period.value[0]) / 1000.0, tz=UTC)
        end_restored = datetime.fromtimestamp(float(restored_period.value[1]) / 1000.0, tz=UTC)
        assert start_restored.date().isoformat() == '2026-01-15'
        assert end_restored.date().isoformat() == '2026-02-20'
    finally:
        if state_path.exists():
            state_path.unlink()


def test_file_state_restores_select_after_options_change() -> None:
    state_path = Path('tests/.tmp_bokeh_file_state_options.json')
    if state_path.exists():
        state_path.unlink()
    try:
        initial_select = Select(value='US2000', options=['US2000', 'NAS100'])
        initial_bindings = [
            BrowserStateBinding('symbol_1', initial_select, kind='select', restore_on_options_change=True),
        ]
        initial_controller = FileStateController(state_path, initial_bindings)
        initial_select.value = 'NAS100'
        initial_controller.persist()

        delayed_select = Select(value='US2000', options=['US2000'])
        delayed_bindings = [
            BrowserStateBinding('symbol_1', delayed_select, kind='select', restore_on_options_change=True),
        ]
        delayed_controller = FileStateController(state_path, delayed_bindings)
        delayed_controller.restore()
        delayed_controller.install_model_watchers()
        assert delayed_select.value == 'US2000'

        delayed_select.options = ['US2000', 'NAS100']
        assert delayed_select.value == 'NAS100'
    finally:
        if state_path.exists():
            state_path.unlink()


def test_file_state_persist_handles_reentrant_calls(monkeypatch) -> None:
    state_path = Path('tests/.tmp_bokeh_file_state_reentrant.json')
    if state_path.exists():
        state_path.unlink()
    original_atomic_write_json = file_state_module._atomic_write_json
    try:
        lookback = Spinner(value=96, low=1, step=1)
        controller = FileStateController(state_path, [BrowserStateBinding('lookback', lookback)])
        writes: list[int] = []

        def fake_atomic_write_json(_path: Path, payload: dict[str, object]) -> None:
            writes.append(int(payload['lookback']))
            if len(writes) == 1:
                lookback.value = 144
                controller.persist()

        monkeypatch.setattr(file_state_module, '_atomic_write_json', fake_atomic_write_json)
        controller.persist()

        assert writes == [96, 144]
    finally:
        monkeypatch.setattr(file_state_module, '_atomic_write_json', original_atomic_write_json)
        if state_path.exists():
            state_path.unlink()


def test_file_state_persist_swallows_write_os_errors(monkeypatch) -> None:
    state_path = Path('tests/.tmp_bokeh_file_state_error.json')
    if state_path.exists():
        state_path.unlink()
    original_atomic_write_json = file_state_module._atomic_write_json
    try:
        lookback = Spinner(value=96, low=1, step=1)
        controller = FileStateController(state_path, [BrowserStateBinding('lookback', lookback)])

        def fake_atomic_write_json(_path: Path, _payload: dict[str, object]) -> None:
            raise PermissionError('locked')

        monkeypatch.setattr(file_state_module, '_atomic_write_json', fake_atomic_write_json)
        controller.persist()
    finally:
        monkeypatch.setattr(file_state_module, '_atomic_write_json', original_atomic_write_json)
        if state_path.exists():
            state_path.unlink()


def test_file_state_restore_skips_fractional_spinner_values() -> None:
    state_path = Path('tests/.tmp_bokeh_file_state_fractional_low.json')
    if state_path.exists():
        state_path.unlink()
    try:
        state_path.write_text('{"opt_entry_start": 0}', encoding='utf-8')
        spinner = Spinner(value=1.5, low=0.1, step=0.1)
        controller = FileStateController(state_path, [BrowserStateBinding('opt_entry_start', spinner)])

        controller.restore()

        assert float(spinner.value) == 1.5
    finally:
        if state_path.exists():
            state_path.unlink()
