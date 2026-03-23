# Tester Metrics Tables

Этот файл фиксирует метрики, которые сейчас показывает локальный tester UI.

Источник:

- [`main.py`](/mnt/w/_python/MT/src/bokeh_app/main.py)
- [`zscore_diagnostics.py`](/mnt/w/_python/MT/src/bokeh_app/zscore_diagnostics.py)

## 1. Equity Summary Overlay

Это компактный блок над графиком equity.

| UI label | Что означает | Единицы / формат |
| --- | --- | --- |
| `symbol_1 / symbol_2` | Текущая тестируемая пара | символы |
| `Trades` | Количество закрытых сделок в тесте | шт. |
| `Gross` | Валовая прибыль до всех торговых издержек | деньги |
| `Spread` | Суммарная стоимость bid/ask spread | деньги |
| `Slip` | Суммарная стоимость slippage | деньги |
| `Comm` | Суммарная комиссия | деньги |
| `Net` | Чистый итог после всех издержек | деньги и `%` к стартовому капиталу |
| `Ending` | Конечный equity | деньги |
| `Max DD` | Максимальная просадка | деньги и `%` от peak equity |
| `Win` | Доля прибыльных сделок | `%` |
| `Cap` | Стартовый капитал | деньги |
| `Peak` | Максимально достигнутый equity | деньги |

## 2. Z-Score Distribution Histogram

| UI element | Что означает | Единицы / формат |
| --- | --- | --- |
| `Z-score` по оси X | Значение z-score | безразмерная величина |
| `Share of Valid Bars` по оси Y | Доля валидных баров, попавших в bin histogram | доля от `0` до `1` |
| чёрная вертикаль | `z = 0` | порог |
| синие вертикали | `±Entry Z` | порог входа |
| зелёные вертикали | `±Exit Z` | порог обычного выхода |
| красные вертикали | `±Stop Z` | порог stop-выхода |

Важно:

| Термин | Смысл |
| --- | --- |
| `Valid Bars` | только бары, где `zscore` конечный, то есть не `NaN` и не `inf` |
| `Share of Valid Bars` | `count_in_bin / total_valid_bars` |
| Сумма всех столбиков histogram | примерно `1.0` |

## 3. Z-Score Metrics Table

Это таблица в блоке `Z-score Metrics`.

| UI metric | Что означает | Формат |
| --- | --- | --- |
| `Valid Bars` | Количество баров с конечным `zscore` | шт. |
| `NaN Bars` | Количество баров без валидного `zscore` | шт. |
| `Last Finite Z` | Последнее конечное значение `zscore` | число |
| `Mean` | Среднее `zscore` | число |
| `Std` | Стандартное отклонение `zscore` | число |
| `Median` | Медиана `zscore` | число |
| `Min` | Минимальный `zscore` | число |
| `Max` | Максимальный `zscore` | число |
| `Abs Mean` | Среднее значение `abs(zscore)` | число |
| `Abs P90` | 90-й перцентиль по `abs(zscore)` | число |
| `Abs P95` | 95-й перцентиль по `abs(zscore)` | число |
| `RMS` | Root mean square для `zscore` | число |
| `Skew` | Асимметрия распределения `zscore` | число |
| `Excess Kurtosis` | Избыточный kurtosis, показатель “тяжести хвостов” | число |
| `P01` | 1-й перцентиль `zscore` | число |
| `P05` | 5-й перцентиль `zscore` | число |
| `P10` | 10-й перцентиль `zscore` | число |
| `P25` | 25-й перцентиль `zscore` | число |
| `P75` | 75-й перцентиль `zscore` | число |
| `P90` | 90-й перцентиль `zscore` | число |
| `P95` | 95-й перцентиль `zscore` | число |
| `P99` | 99-й перцентиль `zscore` | число |
| `Positive Share` | Доля валидных баров, где `z > 0` | `%` |
| `Negative Share` | Доля валидных баров, где `z < 0` | `%` |
| `|z| <= 1.0` | Доля баров внутри тихого режима | `%` |
| `|z| <= 2.0` | Доля баров внутри диапазона `±2 sigma` | `%` |
| `|z| >= 2.0` | Доля баров за пределами `±2 sigma` | `%` |
| `|z| >= 3.0` | Доля баров за пределами `±3 sigma` | `%` |
| `|z| >= Entry` | Доля баров, где достигнут entry-threshold | `%` |
| `Zero Crossings` | Сколько раз знак `zscore` менялся между соседними валидными барами | шт. |

## 4. Exit-Dependent Metrics

Эти строки зависят от того, какой режим выхода выбран.

### 4.1. Когда `Exit Z >= 0`

| UI metric | Что означает | Формат |
| --- | --- | --- |
| `|z| <= Exit` | Доля баров внутри mean-reversion зоны выхода `|z| <= exit_threshold` | `%` |

### 4.2. Когда `Exit Z < 0`

| UI metric | Что означает | Формат |
| --- | --- | --- |
| `z >= +|Exit|` | Доля баров, где есть opposite-signal порог для short-spread логики | `%` |
| `z <= -|Exit|` | Доля баров, где есть opposite-signal порог для long-spread логики | `%` |

## 5. Stop-Dependent Metric

Эта строка появляется только когда stop включён.

| UI metric | Что означает | Формат |
| --- | --- | --- |
| `|z| >= Stop` | Доля баров, где достигнут stop-threshold | `%` |

## 6. Короткие формулы

| Метрика | Формула |
| --- | --- |
| `Net` | `gross_pnl - spread_cost - slippage_cost - commission_cost` |
| `Ending` | `initial_capital + net_pnl` |
| `Win` | `winning_trades / total_trades` |
| `Share of Valid Bars` | `count_in_bin / total_valid_bars` |
| `Positive Share` | `count(z > 0) / valid_bars` |
| `Negative Share` | `count(z < 0) / valid_bars` |
| `|z| >= Entry` | `count(abs(z) >= entry_threshold) / valid_bars` |
| `|z| <= Exit` | `count(abs(z) <= exit_threshold) / valid_bars` |
| `|z| >= Stop` | `count(abs(z) >= stop_threshold) / valid_bars` |

## 7. Примечание по скрину

Список выше сохранён не “на глаз” со скрина, а по текущему локальному коду UI и вычислений.

Это значит:

- названия метрик соответствуют текущему приложению
- смысл метрик соответствует текущей реализации
- если UI потом изменится, этот файл тоже надо обновлять
