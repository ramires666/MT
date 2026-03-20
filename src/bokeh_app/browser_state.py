from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Sequence

from bokeh.document import Document
from bokeh.events import DocumentReady
from bokeh.models import Button, CustomJS, Model


@dataclass(slots=True)
class BrowserStateBinding:
    state_key: str
    model: Model
    property_name: str = "value"
    kind: str = "value"
    default: Any = None
    restore_on_options_change: bool = False


def _serialize_default(value: Any) -> Any:
    if isinstance(value, datetime):
        current = value if value.tzinfo else value.replace(tzinfo=UTC)
        return current.timestamp() * 1000.0
    if isinstance(value, tuple):
        return [_serialize_default(item) for item in value]
    if isinstance(value, list):
        return [_serialize_default(item) for item in value]
    return value


def _storage_helpers(storage_key: str) -> str:
    return f"""
const STORAGE_KEY = {json.dumps(storage_key)};
window.__mt_pair_state_restoring = window.__mt_pair_state_restoring || false;
function readState() {{
  try {{
    return JSON.parse(window.localStorage.getItem(STORAGE_KEY) || "{{}}");
  }} catch (error) {{
    console.warn("Failed to parse browser state", error);
    return {{}};
  }}
}}
function writeState(state) {{
  try {{
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  }} catch (error) {{
    console.warn("Failed to persist browser state", error);
  }}
}}
"""


def _save_callback_code(storage_key: str, state_key: str, property_name: str) -> str:
    return (
        _storage_helpers(storage_key)
        + f"""
if (window.__mt_pair_state_restoring) {{
  return;
}}
const state = readState();
let value = model.{property_name};
if (Array.isArray(value)) {{
  value = value.slice();
}}
state[{json.dumps(state_key)}] = value;
writeState(state);
"""
    )


def _options_restore_code(storage_key: str, state_key: str) -> str:
    return (
        _storage_helpers(storage_key)
        + f"""
const state = readState();
const desired = state[{json.dumps(state_key)}];
if (desired == null) {{
  return;
}}
const options = (model.options || []).map((item) => typeof item === "string" ? item : item.value);
if (options.includes(desired) && model.value !== desired) {{
  model.value = desired;
}}
"""
    )


def _restore_assignment(arg_name: str, binding: BrowserStateBinding) -> str:
    state_key_literal = json.dumps(binding.state_key)
    property_name = binding.property_name
    if binding.kind == "select":
        return f"""
(() => {{
  const desired = state[{state_key_literal}];
  if (desired == null) {{
    return;
  }}
  const options = ({arg_name}.options || []).map((item) => typeof item === "string" ? item : item.value);
  if (options.includes(desired)) {{
    {arg_name}.{property_name} = desired;
  }}
}})();
"""
    if binding.kind == "range":
        return f"""
if (Array.isArray(state[{state_key_literal}])) {{
  {arg_name}.{property_name} = state[{state_key_literal}].slice();
}}
"""
    if binding.kind == "visible":
        return f"""
if (typeof state[{state_key_literal}] === "boolean") {{
  {arg_name}.{property_name} = state[{state_key_literal}];
}}
"""
    return f"""
if (state[{state_key_literal}] !== undefined) {{
  {arg_name}.{property_name} = state[{state_key_literal}];
}}
"""


def _save_assignment(arg_name: str, binding: BrowserStateBinding) -> str:
    state_key_literal = json.dumps(binding.state_key)
    property_name = binding.property_name
    return f"""
(() => {{
  let value = {arg_name}.{property_name};
  if (Array.isArray(value)) {{
    value = value.slice();
  }}
  state[{state_key_literal}] = value;
}})();
"""


def _needs_numeric_dom_persistence(binding: BrowserStateBinding) -> bool:
    return binding.property_name == "value" and type(binding.model).__name__ == "Spinner"


def attach_browser_state(
    event_target: Document,
    bindings: Sequence[BrowserStateBinding],
    *,
    storage_key: str,
    reset_button: Button | None = None,
    restore_delay_ms: int = 1500,
) -> dict[str, Any]:
    defaults = {
        binding.state_key: _serialize_default(binding.default if binding.default is not None else getattr(binding.model, binding.property_name))
        for binding in bindings
    }

    for binding in bindings:
        binding.model.js_on_change(
            binding.property_name,
            CustomJS(args={"model": binding.model}, code=_save_callback_code(storage_key, binding.state_key, binding.property_name)),
        )
        if binding.property_name == "value" and "value_throttled" in binding.model.properties():
            binding.model.js_on_change(
                "value_throttled",
                CustomJS(args={"model": binding.model}, code=_save_callback_code(storage_key, binding.state_key, "value_throttled")),
            )
        if binding.kind == "select" and binding.restore_on_options_change:
            binding.model.js_on_change(
                "options",
                CustomJS(args={"model": binding.model}, code=_options_restore_code(storage_key, binding.state_key)),
            )

    restore_args: dict[str, Any] = {}
    sticky_restore_lines: list[str] = []
    restore_lines = [
        _storage_helpers(storage_key),
        f"const defaults = {json.dumps(defaults)};",
        "function mergedState() { return Object.assign({}, defaults, readState()); }",
        "function pushChildViews(stack, view) {",
        "  if (view == null) { return; }",
        "  const child_views = view.child_views != null ? view.child_views : view._child_views;",
        "  if (child_views == null) { return; }",
        "  if (Array.isArray(child_views)) { for (const child of child_views) { stack.push(child); } return; }",
        "  if (typeof child_views.values === 'function') { for (const child of child_views.values()) { stack.push(child); } return; }",
        "  if (typeof child_views[Symbol.iterator] === 'function') { for (const child of child_views) { stack.push(child); } }",
        "}",
        "function findView(model_id) {",
        "  const root_views = Object.values(Bokeh.index || {});",
        "  const stack = root_views.slice();",
        "  while (stack.length > 0) {",
        "    const view = stack.pop();",
        "    if (view == null) { continue; }",
        "    if (view.model != null && view.model.id === model_id) { return view; }",
        "    pushChildViews(stack, view);",
        "  }",
        "  return null;",
        "}",
        "function bindNumericInputPersistence(model, state_key) {",
        "  const persistInput = (input_el) => {",
        "    if (window.__mt_pair_state_restoring || input_el == null) { return; }",
        "    const raw_value = input_el.value;",
        "    if (raw_value == null || raw_value === '') { return; }",
        "    const numeric_value = Number(raw_value);",
        "    if (!Number.isFinite(numeric_value)) { return; }",
        "    const state = readState();",
        "    state[state_key] = numeric_value;",
        "    writeState(state);",
        "  };",
        "  const attach = () => {",
        "    const view = findView(model.id);",
        "    const input_el = view != null ? (view.input_el != null ? view.input_el : (view.el != null ? view.el.querySelector('input') : null)) : null;",
        "    if (input_el == null) { return false; }",
        "    if (input_el.__mt_pair_state_bound) { return true; }",
        "    const handler = () => persistInput(input_el);",
        "    input_el.addEventListener('input', handler);",
        "    input_el.addEventListener('change', handler);",
        "    input_el.addEventListener('blur', handler);",
        "    input_el.addEventListener('wheel', handler, {passive: true});",
        "    input_el.addEventListener('keydown', (event) => { if (event.key === 'Enter' || event.key === 'Tab') { handler(); } });",
        "    input_el.__mt_pair_state_bound = true;",
        "    return true;",
        "  };",
        "  if (!attach()) {",
        "    window.setTimeout(attach, 250);",
        "    window.setTimeout(attach, 1000);",
        "    window.setTimeout(attach, 2000);",
        "  }",
        "}",
        "window.__mt_pair_state_restoring = true;",
        "function applyState(state) {",
    ]
    save_lines = [
        _storage_helpers(storage_key),
        "window.__mt_pair_state_save_all = () => {",
        "  const state = readState();",
        "  if (window.__mt_pair_state_restoring) { return; }",
    ]
    numeric_dom_bind_lines: list[str] = []
    for index, binding in enumerate(bindings):
        arg_name = f"model_{index}"
        restore_args[arg_name] = binding.model
        restore_lines.append(_restore_assignment(arg_name, binding))
        save_lines.append(_save_assignment(arg_name, binding))
        sticky_restore_lines.append(_restore_assignment(arg_name, binding))
        if _needs_numeric_dom_persistence(binding):
            numeric_dom_bind_lines.append(f"bindNumericInputPersistence({arg_name}, {json.dumps(binding.state_key)});")
    restore_lines.append("}")
    restore_lines.extend(
        [
            "function applyStickyState(state) {",
            *sticky_restore_lines,
            "}",
            "applyState(mergedState());",
            "applyStickyState(mergedState());",
            "window.setTimeout(() => applyState(mergedState()), 250);",
            "window.setTimeout(() => applyStickyState(mergedState()), 400);",
            "window.setTimeout(() => applyState(mergedState()), 1000);",
            "window.setTimeout(() => applyStickyState(mergedState()), 1400);",
            "window.setTimeout(() => applyStickyState(mergedState()), 2500);",
            "window.setTimeout(() => applyStickyState(mergedState()), 5000);",
        ]
    )
    restore_lines.extend(numeric_dom_bind_lines)
    save_lines.extend(
        [
            "  writeState(state);",
            "};",
            "if (!window.__mt_pair_state_save_all_bound) {",
            "  const persistNow = () => { if (window.__mt_pair_state_save_all) { window.__mt_pair_state_save_all(); } };",
            "  const restoreStickyNow = () => { if (typeof applyStickyState === 'function') { applyStickyState(mergedState()); } };",
            "  window.addEventListener('beforeunload', persistNow);",
            "  window.addEventListener('pagehide', persistNow);",
            "  window.addEventListener('blur', persistNow);",
            "  window.addEventListener('pageshow', restoreStickyNow);",
            "  window.addEventListener('focus', restoreStickyNow);",
            "  document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'hidden') { persistNow(); } else { restoreStickyNow(); } });",
            "  window.setInterval(persistNow, 2000);",
            "  window.__mt_pair_state_save_all_bound = true;",
            "}",
        ]
    )
    restore_lines.extend(save_lines)
    restore_lines.append(
        f"window.setTimeout(() => {{ applyState(mergedState()); window.__mt_pair_state_restoring = false; if (window.__mt_pair_state_save_all) {{ window.__mt_pair_state_save_all(); }} }}, {int(restore_delay_ms)});"
    )
    event_target.js_on_event(DocumentReady, CustomJS(args=restore_args, code="\n".join(restore_lines)))

    if reset_button is not None:
        reset_button.js_on_click(
            CustomJS(
                code=(
                    _storage_helpers(storage_key)
                    + "window.__mt_pair_state_restoring = false;\n"
                    + "try { window.localStorage.removeItem(STORAGE_KEY); } catch (error) { console.warn('Failed to clear browser state', error); }\n"
                )
            )
        )

    return defaults
