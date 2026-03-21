from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

from bokeh.models import Model

from bokeh_app.browser_state import BrowserStateBinding



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
    return [item if isinstance(item, str) else str(getattr(item, 'value', item.get('value'))) for item in options]



def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}



def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + '.tmp')
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding='utf-8')
    temp_path.replace(path)


@dataclass(slots=True)
class FileStateController:
    state_path: Path
    bindings: Sequence[BrowserStateBinding]
    defaults: dict[str, Any] = field(init=False)
    _restoring: bool = field(default=False, init=False)
    _loaded_state: dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.defaults = {
            binding.state_key: _serialize_value(
                binding.default if binding.default is not None else getattr(binding.model, binding.property_name)
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
            binding.state_key: _serialize_value(getattr(binding.model, binding.property_name))
            for binding in self.bindings
        }

    def persist(self) -> None:
        if self._restoring:
            return
        payload = self.snapshot()
        _atomic_write_json(self.state_path, payload)
        self._loaded_state = payload

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
