"""Отчёт по витрине: таблицы в консоль и одна самодостаточная HTML-страница.

Не Streamlit-сервис, который нужно держать запущенным: дежурному по теплосети удобнее файл — страница собирается из
витрины, открывается двойным щелчком
и не требует ни сервера, ни интернета.
"""

from __future__ import annotations

import html
from collections.abc import Sequence
from datetime import UTC, datetime

from teplo.alerts import Alert
from teplo.meteo import Outside, norm_supply_c
from teplo.warehouse import NodeState, Run, WindowRow


def table(headers: Sequence[str], rows: Sequence[Sequence[str]], align: str = "") -> str:
    """Простая таблица с выравниванием по столбцам: «l» — влево, «r» — вправо."""
    columns = len(headers)
    widths = [len(header) for header in headers]
    for row in rows:
        for index in range(columns):
            widths[index] = max(widths[index], len(row[index]))
    rules = (align + "l" * columns)[:columns]

    def render(cells: Sequence[str]) -> str:
        parts = [
            cells[index].rjust(widths[index]) if rules[index] == "r" else cells[index].ljust(widths[index])
            for index in range(columns)
        ]
        return "  ".join(parts).rstrip()

    return "\n".join([render(headers), *(render(row) for row in rows)])


def nodes_table(states: Sequence[NodeState]) -> str:
    rows = [
        [
            state.node,
            _hhmm(state.window_start),
            str(state.readings),
            f"{state.energy_kwh:.1f}",
            f"{state.volume_m3:.2f}",
            f"{state.avg_supply:.1f}",
            f"{state.avg_return:.1f}",
            f"{state.avg_delta:.1f}",
            f"{state.pressure_min:.2f}",
            str(state.late),
        ]
        for state in states
    ]
    headers = ["Узел", "Окно", "Показаний", "кВт·ч", "м³", "Подача", "Обратка", "Перепад", "Давление", "Поздних"]
    return table(headers, rows, align="ll" + "r" * 8)


def windows_table(windows: Sequence[WindowRow]) -> str:
    rows = [
        [
            _hhmm(item.window_start),
            item.node,
            str(item.readings),
            f"{item.energy_kwh:.1f}",
            f"{item.avg_supply:.1f}",
            f"{item.avg_delta:.1f}",
            str(item.late),
        ]
        for item in windows
    ]
    return table(["Окно", "Узел", "Показаний", "кВт·ч", "Подача", "Перепад", "Поздних"], rows, align="ll" + "r" * 5)


def runs_table(runs: Sequence[Run]) -> str:
    rows = [
        [
            str(run.run_id),
            _hhmm(run.started_at),
            str(run.events_read),
            str(run.events_stored),
            str(run.duplicates),
            str(run.windows_touched),
            str(run.late),
            f"{run.seconds:.2f}",
        ]
        for run in runs
    ]
    headers = ["№", "Время", "Прочитано", "Записано", "Повторов", "Окон", "Поздних", "Секунд"]
    return table(headers, rows, align="ll" + "r" * 6)


def alerts_block(alerts: Sequence[Alert]) -> str:
    if not alerts:
        return "Аварий и предупреждений нет."
    return "\n".join(alert.line() for alert in alerts)


def summary(
    *,
    states: Sequence[NodeState],
    alerts: Sequence[Alert],
    totals: dict[str, float],
    outside: Outside | None,
    runs: Sequence[Run] = (),
) -> str:
    parts = [_weather_line(outside), "", nodes_table(states), "", alerts_block(alerts), "", _totals_line(totals)]
    if runs:
        parts += ["", "Проходы конвейера:", runs_table(runs)]
    return "\n".join(parts)


def _weather_line(outside: Outside | None) -> str:
    if outside is None:
        return "Наружная температура неизвестна — правило «недотоп» отключено."
    norm = norm_supply_c(outside.temperature_c)
    return (
        f"На улице {outside.temperature_c:.1f} °C ({outside.source}, {_hhmm(outside.measured_at)}) — "
        f"по графику подача должна быть около {norm:.1f} °C."
    )


def _totals_line(totals: dict[str, float]) -> str:
    parts = [
        f"узлов {int(totals.get('узлов', 0))}",
        f"окон {int(totals.get('окон', 0))}",
        f"показаний {int(totals.get('показаний', 0))}",
        f"опоздавших {int(totals.get('опоздавших', 0))}",
        f"тепла {totals.get('энергия', 0.0):.1f} кВт·ч",
    ]
    return "Витрина: " + ", ".join(parts) + "."


# ── HTML ────────────────────────────────────────────────────────────────────────────────────────

STYLE = """
:root { color-scheme: light dark; --ink:#14181d; --ink-2:#5a646e; --line:#dfe3e8; --bg:#f7f8fa;
        --card:#ffffff; --alarm:#b3261e; --warn:#8a6100; --ok:#1f6b3a; }
@media (prefers-color-scheme: dark) {
  :root { --ink:#e8ecf1; --ink-2:#9aa4ae; --line:#2b3239; --bg:#12161a; --card:#1a1f25;
          --alarm:#ff8a80; --warn:#ffcc80; --ok:#8fd6a8; }
}
* { box-sizing: border-box; }
body { margin:0; padding:24px 16px 48px; background:var(--bg); color:var(--ink);
       font:15px/1.5 "Segoe UI", Roboto, Arial, sans-serif; }
main { max-width: 980px; margin: 0 auto; }
h1 { font-size: 1.5rem; margin: 0 0 4px; }
.sub { color: var(--ink-2); margin: 0 0 24px; }
.card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:16px; margin-bottom:16px; }
.card h2 { font-size:1.05rem; margin:0 0 12px; }
.alert { display:flex; gap:10px; padding:8px 0; border-bottom:1px solid var(--line); }
.alert:last-child { border-bottom:0; }
.tag { flex:0 0 auto; font-size:.78rem; padding:2px 8px; border-radius:999px; border:1px solid currentColor; }
.alarm { color:var(--alarm); } .warn { color:var(--warn); } .ok { color:var(--ok); }
.node { display:grid; grid-template-columns: minmax(180px,1fr) auto; gap:12px; align-items:center;
        padding:12px 0; border-bottom:1px solid var(--line); }
.node:last-child { border-bottom:0; }
.node b { display:block; }
.node span { color:var(--ink-2); font-size:.88rem; }
svg { display:block; max-width:100%; height:auto; }
table { border-collapse:collapse; width:100%; font-size:.9rem; }
th, td { text-align:right; padding:6px 8px; border-bottom:1px solid var(--line); font-variant-numeric: tabular-nums; }
th:first-child, td:first-child { text-align:left; }
@media (max-width:560px) { .node { grid-template-columns:1fr; } }
"""


def dashboard(
    *,
    states: Sequence[NodeState],
    alerts: Sequence[Alert],
    windows: Sequence[WindowRow],
    outside: Outside | None,
    totals: dict[str, float],
    generated_at: datetime | None = None,
) -> str:
    """Самодостаточная страница: сводка, аварии, по каждому узлу — столбики тепла по окнам."""
    moment = generated_at or datetime.now(UTC)
    blocks = [
        "<h1>Теплосеть: сводка по окнам</h1>",
        f'<p class="sub">{html.escape(_weather_line(outside))} Собрано {_hhmm(moment)} UTC.</p>',
        _alerts_card(alerts),
        _nodes_card(states, windows),
        _totals_card(totals),
    ]
    body = "\n".join(blocks)
    return (
        '<!doctype html>\n<html lang="ru">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        "<title>Теплосеть: сводка по окнам</title>\n"
        f"<style>{STYLE}</style>\n</head>\n<body>\n<main>\n{body}\n</main>\n</body>\n</html>\n"
    )


def _alerts_card(alerts: Sequence[Alert]) -> str:
    if not alerts:
        return '<section class="card"><h2>Аварии</h2><p class="ok">Всё спокойно.</p></section>'
    rows = "\n".join(
        f'<div class="alert"><span class="tag {"alarm" if alert.level == "авария" else "warn"}">'
        f"{html.escape(alert.rule)}</span><div><b>{html.escape(alert.node)}</b><br>"
        f"<span>{html.escape(alert.text)}</span></div></div>"
        for alert in alerts
    )
    return f'<section class="card"><h2>Аварии и предупреждения</h2>{rows}</section>'


def _nodes_card(states: Sequence[NodeState], windows: Sequence[WindowRow]) -> str:
    rows = []
    for state in states:
        series = [item.energy_kwh for item in windows if item.node == state.node]
        rows.append(
            '<div class="node"><div>'
            f"<b>{html.escape(state.node)}</b>"
            f"<span>последнее окно {_hhmm(state.window_start)} · {state.energy_kwh:.1f} кВт·ч · "
            f"подача {state.avg_supply:.1f} °C · перепад {state.avg_delta:.1f} °C · "
            f"давление {state.pressure_min:.2f} бар</span>"
            f"</div>{_sparkline(series)}</div>"
        )
    return f'<section class="card"><h2>Узлы</h2>{"".join(rows)}</section>'


def _sparkline(values: Sequence[float], width: int = 260, height: int = 48) -> str:
    """Столбики тепла по окнам: без библиотек, чтобы страница открывалась без интернета."""
    if not values:
        return f'<svg width="{width}" height="{height}" role="img" aria-label="нет данных"></svg>'
    top = max(values) or 1.0
    step = width / len(values)
    bars = []
    for index, value in enumerate(values):
        bar_height = max(2.0, value / top * (height - 4))
        x = index * step
        y = height - bar_height
        bars.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{max(step - 2, 1):.1f}" height="{bar_height:.1f}" '
            f'rx="2" fill="currentColor" opacity="0.65"><title>{value:.1f} кВт·ч</title></rect>'
        )
    return (
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="тепло по окнам">{"".join(bars)}</svg>'
    )


def _totals_card(totals: dict[str, float]) -> str:
    rows = "\n".join(
        f"<tr><td>{html.escape(name)}</td><td>{value:.1f}</td></tr>"
        if name == "энергия"
        else f"<tr><td>{html.escape(name)}</td><td>{int(value)}</td></tr>"
        for name, value in totals.items()
    )
    return f'<section class="card"><h2>Витрина целиком</h2><table>{rows}</table></section>'


def _hhmm(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%H:%M")
