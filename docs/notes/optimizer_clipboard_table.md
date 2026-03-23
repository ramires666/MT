# Optimizer Clipboard Table

Этот документ фиксирует именно формат копирования из таблицы `Optimization Results` в UI.

Важно:

- В clipboard попадают только **видимые колонки** таблицы optimizer.
- Скрытые поля вроде `trial_id`, `objective_metric`, `objective_score` и `bollinger_k` из такого дампа **не восстанавливаются**, потому что их нет в видимой таблице.
- Порядок колонок берётся из UI в [`main.py`](/mnt/w/_python/MT/src/bokeh_app/main.py).

## Visible Columns

### Without `Stop Z`

| # | Column |
| --- | --- |
| 1 | Net |
| 2 | Max DD |
| 3 | Trades |
| 4 | Lookback |
| 5 | Omega |
| 6 | K |
| 7 | Score |
| 8 | Ending |
| 9 | PnL/DD |
| 10 | Ulcer |
| 11 | UPI |
| 12 | Entry Z |
| 13 | Exit Z |
| 14 | Win |
| 15 | Gross |
| 16 | Spread |
| 17 | Slip |
| 18 | Comm |
| 19 | Costs |

### With `Stop Z`

| # | Column |
| --- | --- |
| 1 | Net |
| 2 | Max DD |
| 3 | Trades |
| 4 | Lookback |
| 5 | Omega |
| 6 | K |
| 7 | Score |
| 8 | Ending |
| 9 | PnL/DD |
| 10 | Ulcer |
| 11 | UPI |
| 12 | Entry Z |
| 13 | Exit Z |
| 14 | Stop Z |
| 15 | Win |
| 16 | Gross |
| 17 | Spread |
| 18 | Slip |
| 19 | Comm |
| 20 | Costs |

## What Each Column Means

| Column | Meaning |
| --- | --- |
| Net | Post-cost net profit for the tested period. |
| Max DD | Maximum drawdown on the tested period. Negative number. |
| Trades | Number of closed trades. |
| Lookback | `lookback_bars` used for spread mean/std and z-score. |
| Omega | `omega_ratio`. |
| K | `k_ratio`. |
| Score | `score_log_trades`. |
| Ending | Final account equity after the tested period. |
| PnL/DD | `net_profit / abs(max_drawdown)` with the engine's finite guards. |
| Ulcer | `ulcer_index`. Lower is better. |
| UPI | `ulcer_performance`. |
| Entry Z | Entry threshold for `abs(zscore)`. |
| Exit Z | Exit threshold. Negative means opposite-signal exit mode. |
| Stop Z | Optional stop threshold. Missing when stop mode is disabled. |
| Win | Win rate as a share in `[0, 1]`, not percent. |
| Gross | Gross PnL before costs. |
| Spread | Total spread cost. |
| Slip | Total slippage cost. |
| Comm | Total commission cost. |
| Costs | `Spread + Slip + Comm`. |

## Clipboard Shapes That Can Be Recovered

### 1. TSV with header row

```text
Net	Max DD	Trades	Lookback	...	Costs
248.62	-366.22	2	32	...	23.11
```

### 2. TSV without header row

```text
248.62	-366.22	2	32	1.174	0.118	0.332	10248.62	0.679	1.4312	0.174	2.0	0.5	0.500	271.73	7.10	4.01	12.00	23.11
```

### 3. Vertical key/value block

```text
Net
248.62
Max DD
-366.22
...
Costs
23.11
```

## Conversion Tool

Есть отдельный конвертер:

[`optimizer_clipboard_to_markdown.py`](/mnt/w/_python/MT/src/tools/optimizer_clipboard_to_markdown.py)

Примеры:

```bash
python src/tools/optimizer_clipboard_to_markdown.py --input tmp/optimizer_clipboard.txt
```

или через stdin:

```bash
pbpaste | python src/tools/optimizer_clipboard_to_markdown.py
```

## Example Markdown Output

| Net | Max DD | Trades | Lookback | Omega | K | Score | Ending | PnL/DD | Ulcer | UPI | Entry Z | Exit Z | Win | Gross | Spread | Slip | Comm | Costs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 248.62 | -366.22 | 2 | 32 | 1.174 | 0.118 | 0.332 | 10248.62 | 0.679 | 1.4312 | 0.174 | 2.0 | 0.5 | 0.500 | 271.73 | 7.10 | 4.01 | 12.00 | 23.11 |
