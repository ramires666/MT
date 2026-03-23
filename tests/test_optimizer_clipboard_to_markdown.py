from tools.optimizer_clipboard_to_markdown import (
    OPTIMIZER_COLUMNS_NO_STOP,
    OPTIMIZER_COLUMNS_WITH_STOP,
    parse_optimizer_clipboard,
    render_markdown_table,
)


def test_parse_optimizer_clipboard_tsv_with_header() -> None:
    raw = (
        "Net\tMax DD\tTrades\tLookback\tOmega\tK\tScore\tEnding\tPnL/DD\tUlcer\tUPI\tEntry Z\tExit Z\tWin\tGross\tSpread\tSlip\tComm\tCosts\n"
        "248.62\t-366.22\t2\t32\t1.174\t0.118\t0.332\t10248.62\t0.679\t1.4312\t0.174\t2.0\t0.5\t0.500\t271.73\t7.10\t4.01\t12.00\t23.11\n"
    )

    columns, rows = parse_optimizer_clipboard(raw)

    assert columns == OPTIMIZER_COLUMNS_NO_STOP
    assert rows == [
        {
            "Net": "248.62",
            "Max DD": "-366.22",
            "Trades": "2",
            "Lookback": "32",
            "Omega": "1.174",
            "K": "0.118",
            "Score": "0.332",
            "Ending": "10248.62",
            "PnL/DD": "0.679",
            "Ulcer": "1.4312",
            "UPI": "0.174",
            "Entry Z": "2.0",
            "Exit Z": "0.5",
            "Win": "0.500",
            "Gross": "271.73",
            "Spread": "7.10",
            "Slip": "4.01",
            "Comm": "12.00",
            "Costs": "23.11",
        }
    ]


def test_parse_optimizer_clipboard_tsv_without_header_and_stop_z() -> None:
    raw = (
        "248.62\t-366.22\t2\t32\t1.174\t0.118\t0.332\t10248.62\t0.679\t1.4312\t0.174\t2.0\t0.5\t3.0\t0.500\t271.73\t7.10\t4.01\t12.00\t23.11\n"
        "120.00\t-100.00\t3\t48\t1.250\t0.145\t0.441\t10120.00\t1.200\t0.9500\t0.221\t1.8\t-1.0\t3.5\t0.667\t150.00\t10.00\t5.00\t15.00\t30.00\n"
    )

    columns, rows = parse_optimizer_clipboard(raw)

    assert columns == OPTIMIZER_COLUMNS_WITH_STOP
    assert rows[0]["Stop Z"] == "3.0"
    assert rows[1]["Exit Z"] == "-1.0"


def test_parse_optimizer_clipboard_vertical_key_value_block() -> None:
    raw = """
Net
248.62
Max DD
-366.22
Trades
2
Lookback
32
Omega
1.174
K
0.118
Score
0.332
Ending
10248.62
PnL/DD
0.679
Ulcer
1.4312
UPI
0.174
Entry Z
2.0
Exit Z
0.5
Win
0.500
Gross
271.73
Spread
7.10
Slip
4.01
Comm
12.00
Costs
23.11
"""

    columns, rows = parse_optimizer_clipboard(raw)

    assert columns == OPTIMIZER_COLUMNS_NO_STOP
    assert rows[0]["Net"] == "248.62"
    assert rows[0]["Costs"] == "23.11"


def test_render_markdown_table_outputs_expected_header() -> None:
    markdown = render_markdown_table(
        OPTIMIZER_COLUMNS_NO_STOP,
        [{"Net": "248.62", "Max DD": "-366.22", "Trades": "2"}],
    )

    assert markdown.splitlines()[0].startswith("| Net | Max DD | Trades |")
    assert markdown.splitlines()[1].startswith("| --- | --- | --- |")
