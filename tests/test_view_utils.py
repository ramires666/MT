from bokeh.layouts import column
from bokeh.models import Button, Div

from bokeh_app.view_utils import sync_toggle_button_types


def test_sync_toggle_button_types_matches_body_visibility() -> None:
    visible_body = Div(visible=True)
    hidden_body = column(visible=False)
    visible_toggle = Button(label="Visible", button_type="default")
    hidden_toggle = Button(label="Hidden", button_type="primary")

    sync_toggle_button_types(
        [
            (visible_body, visible_toggle),
            (hidden_body, hidden_toggle),
        ]
    )

    assert visible_toggle.button_type == "primary"
    assert hidden_toggle.button_type == "default"
