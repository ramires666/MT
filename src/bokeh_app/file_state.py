from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from math import isfinite
from pathlib import Path
from typing import Any, Sequence

from bokeh.models import Model, Spinner

from bokeh_app.browser_state import BrowserStateBinding
from bokeh_app.numeric_inputs import has_fractional_step, normalize_fractional_value



def _serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        current = value if value.tzinfo else value.replace(tzinfo=UTC)
        return current.timestamp() * 1000.0
    if isinstance(value, tuple):
        return [_serialize_value(item) for item in value]
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    return value



def _deserialize_range_value(value: Any) -> Any:
    if not isinstance(value, (list, tuple)):
        return value
    restored: list[Any] = []
    for item in value:
        if isinstance(item, (int, float)):
            restored.append(datetime.fromtimestamp(float(item) / 1000.0, tz=UTC).replace(tzinfo=None))
        else:
            restored.append(item)
    return tuple(restored)



def _select_options(model: Model) -> list[str]:
    options = getattr(model, 'options', None) or []
    values: list[str] = []
    for item in options:
        if isinstance(item, str):
            values.append(item)
            continue
        if isinstance(item, (tuple, list)) and item:
            values.append(str(item[0]))
            continue
        if isinstance(item, dict) and 'value' in item:
            values.append(str(item['value']))
            continue
        value = getattr(item, 'value', None)
        if value is not None:
            values.append(str(value))
    return values


def _sanitize_spinner_value(model: Spinner, value: Any) -> Any:
    if value in (None, ''):
        return value
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return value
    if not isfinite(numeric_value):
        return value

    if has_fractional_step(getattr(model, 'step', None)):
        normalized = normalize_fractional_value(
            numeric_value,
            step=model.step,
            low=model.low,
            high=model.high,
        )
        return value if normalized is None else normalized

    normalized_int = int(round(numeric_value))
    if model.low is not None:
        try:
            normalized_int = max(normalized_int, int(round(float(model.low))))
        except (TypeError, ValueError):
            pass
    if model.high is not None:
        try:
            normalized_int = min(normalized_int, int(round(float(model.high))))
        except (TypeError, ValueError):
            pass
    return normalized_int


def _binding_value(binding: BrowserStateBinding) -> Any:
    value = getattr(binding.model, binding.property_name)
    if binding.property_name == 'value' and isinstance(binding.model, Spinner):
        value = _sanitize_spinner_value(binding.model, value)
    return value



def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _cleanup_temp_files(path: Path) -> None:
    parent = path.parent
    if not parent.exists():
        return
    for pattern in (f"{path.name}.tmp", f"{path.name}.*.tmp"):
        for candidate in parent.glob(pattern):
            try:
                if candidate != path:
                    candidate.unlink(missing_ok=True)
            except OSError:
                continue



def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _cleanup_temp_files(path)
    serialized = json.dumps(payload, indent=2, sort_keys=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    try:
        temp_path.write_text(serialized, encoding='utf-8')
        for attempt in range(5):
            try:
                temp_path.replace(path)
                return
            except PermissionError:
                if attempt == 4:
                    break
                time.sleep(0.02 * (attempt + 1))
        path.write_text(serialized, encoding='utf-8')
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
        _cleanup_temp_files(path)


@dataclass(slots=True)
class FileStateController:
    state_path: Path
    bindings: Sequence[BrowserStateBinding]
    defaults: dict[str, Any] = field(init=False)
    _restoring: bool = field(default=False, init=False)
    _loaded_state: dict[str, Any] = field(default_factory=dict, init=False)
    _persisting: bool = field(default=False, init=False)
    _persist_queued: bool = field(default=False, init=False)
    _suspend_depth: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        _cleanup_temp_files(self.state_path)
        self.defaults = {
            binding.state_key: _serialize_value(
                binding.default if binding.default is not None else _binding_value(binding)
            )
            for binding in self.bindings
        }
        self._loaded_state = self.read_state()

    def read_state(self) -> dict[str, Any]:
        merged = dict(self.defaults)
        merged.update(_read_json(self.state_path))
        return merged

    def snapshot(self) -> dict[str, Any]:
        return {
            binding.state_key: _serialize_value(_binding_value(binding))
            for binding in self.bindings
        }

    def persist(self) -> None:
        if self._restoring or self._suspend_depth > 0:
            return
        if self._persisting:
            self._persist_queued = True
            return
        self._persisting = True
        try:
            while True:
                self._persist_queued = False
                payload = self.snapshot()
                try:
                    _atomic_write_json(self.state_path, payload)
                except OSError:
                    return
                self._loaded_state = payload
                if not self._persist_queued:
                    return
        finally:
            self._persisting = False

    def restore(self) -> dict[str, Any]:
        state = self.read_state()
        self._restoring = True
        try:
            for binding in self.bindings:
                self._restore_binding(binding, state)
        finally:
            self._restoring = False
        self._loaded_state = state
        return state

    def clear(self) -> None:
        if self.state_path.exists():
            self.state_path.unlink()
        self._loaded_state = dict(self.defaults)

    @contextmanager
    def suspend(self):
        self._suspend_depth += 1
        try:
            yield
        finally:
            self._suspend_depth = max(0, self._suspend_depth - 1)

    def _restore_binding(self, binding: BrowserStateBinding, state: dict[str, Any]) -> None:
        if binding.state_key not in state:
            return
        desired = state[binding.state_key]
        if binding.kind == 'range':
            desired = _deserialize_range_value(desired)
        if binding.kind == 'select':
            options = _select_options(binding.model)
            if desired not in options:
                return
        if binding.property_name == 'value' and isinstance(binding.model, Spinner):
            desired = _sanitize_spinner_value(binding.model, desired)
        try:
            setattr(binding.model, binding.property_name, desired)
        except Exception:
            return

    def install_model_watchers(self) -> None:
        for binding in self.bindings:
            def _handler(_attr: str, _old: Any, _new: Any, self=self) -> None:
                self.persist()
            binding.model.on_change(binding.property_name, _handler)
            if binding.property_name == 'value' and 'value_throttled' in binding.model.properties():
                binding.model.on_change('value_throttled', _handler)
            if binding.kind == 'select' and binding.restore_on_options_change:
                def _options_handler(_attr: str, _old: Any, _new: Any, self=self, binding=binding) -> None:
                    if self._restoring:
                        return
                    current_state = self.read_state()
                    self._restoring = True
                    try:
                        self._restore_binding(binding, current_state)
                    finally:
                        self._restoring = False
                    self.persist()
                binding.model.on_change('options', _options_handler)
