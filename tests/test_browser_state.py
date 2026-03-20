from __future__ import annotations

from bokeh.document import Document
from bokeh.layouts import column
from bokeh.models import Button, Select, Spinner

from bokeh_app.browser_state import BrowserStateBinding, attach_browser_state


def test_attach_browser_state_adds_spinner_dom_persistence() -> None:
    spinner = Spinner(title="Lookback", value=48, step=1)
    selector = Select(title="Mode", value="grid", options=["grid", "genetic"])
    root = column(spinner, selector)
    doc = Document()
    doc.add_root(root)

    attach_browser_state(
        doc,
        [
            BrowserStateBinding("opt_lookback_start", spinner),
            BrowserStateBinding("optimization_mode", selector, kind="select"),
        ],
        storage_key="test.browser.state",
        reset_button=Button(label="Reset"),
    )

    callbacks = doc.callbacks._js_event_callbacks.get("document_ready", [])
    assert callbacks
    code = callbacks[0].code
    assert "bindNumericInputPersistence" in code
    assert "opt_lookback_start" in code
    assert "optimization_mode" in code
    assert "__mt_pair_state_save_all" in code



def test_attach_browser_state_keeps_visible_and_button_bindings() -> None:
    spinner = Spinner(title="Lookback", value=48, step=1)
    toggle = Button(label="Equity", button_type="primary")
    root = column(spinner)
    root.visible = False
    doc = Document()
    doc.add_root(root)

    attach_browser_state(
        doc,
        [
            BrowserStateBinding("show_equity", root, property_name="visible", kind="visible"),
            BrowserStateBinding("show_equity_button", toggle, property_name="button_type", default="primary"),
        ],
        storage_key="test.browser.state",
    )

    code = doc.callbacks._js_event_callbacks["document_ready"][0].code
    assert "show_equity" in code
    assert "show_equity_button" in code
