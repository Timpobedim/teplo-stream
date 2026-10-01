"""Правила, ради которых конвейер вообще нужен: что в теплосети сломалось прямо сейчас.

Данные не заканчиваются таблицей, на которую должен смотреть человек: у витрины есть потребитель — пять правил, каждое
со своим порогом и понятным текстом.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final

from teplo.meteo import Outside, norm_supply_c
from teplo.warehouse import NodeState
from teplo.windows import WINDOW

MIN_DELTA_C: Final = 10.0
"""Перепад меньше десяти градусов — дом берёт мало тепла: либо перетоп, либо плохая циркуляция."""

MIN_PRESSURE_BAR: Final = 4.0
LEAK_RATIO: Final = 1.4
UNDERHEAT_C: Final = 5.0
SILENT_WINDOWS: Final = 3

LEVELS: Final = {"авария": 0, "внимание": 1}


@dataclass(frozen=True, slots=True)
class Alert:
    node: str
    level: str
    rule: str
    text: str
    window_start: datetime | None = None

    def line(self) -> str:
        mark = "!" if self.level == "авария" else "·"
        return f"{mark} {self.node}: {self.text}"


def check(
    states: Sequence[NodeState],
    *,
    now: datetime | None = None,
    outside: Outside | None = None,
    window: float = WINDOW,
) -> list[Alert]:
    """Проверить последнее окно каждого узла. Правила, зависящие от улицы, работают, только если
    наружная температура известна: выдумывать её нельзя."""
    moment = now or datetime.now(UTC)
    alerts: list[Alert] = []
    for state in states:
        alerts.extend(_silence(state, moment, window))
        alerts.extend(_heat(state, outside))
        alerts.extend(_hydraulics(state))
    return sorted(alerts, key=lambda item: (LEVELS.get(item.level, 9), item.node, item.rule))


def _silence(state: NodeState, moment: datetime, window: float) -> list[Alert]:
    silence = moment - state.window_end
    if silence <= timedelta(seconds=window * SILENT_WINDOWS):
        return []
    minutes = int(silence.total_seconds() // 60)
    return [
        Alert(
            node=state.node,
            level="авария",
            rule="нет связи",
            text=f"нет показаний {minutes} мин (последнее окно {_hhmm(state.window_start)})",
            window_start=state.window_start,
        )
    ]


def _heat(state: NodeState, outside: Outside | None) -> list[Alert]:
    if outside is None:
        return []
    norm = norm_supply_c(outside.temperature_c)
    if state.avg_supply >= norm - UNDERHEAT_C:
        return []
    return [
        Alert(
            node=state.node,
            level="внимание",
            rule="недотоп",
            text=(f"подача {state.avg_supply:.1f} °C при норме {norm:.1f} °C для улицы {outside.temperature_c:.1f} °C"),
            window_start=state.window_start,
        )
    ]


def _hydraulics(state: NodeState) -> list[Alert]:
    alerts: list[Alert] = []
    if state.avg_delta < MIN_DELTA_C:
        alerts.append(
            Alert(
                node=state.node,
                level="внимание",
                rule="малый перепад",
                text=(
                    f"перепад {state.avg_delta:.1f} °C (подача {state.avg_supply:.1f}, "
                    f"обратка {state.avg_return:.1f}) — тепло уходит обратно в сеть"
                ),
                window_start=state.window_start,
            )
        )
    if state.windows > 2 and state.baseline_volume > 0 and state.volume_m3 > state.baseline_volume * LEAK_RATIO:
        ratio = state.volume_m3 / state.baseline_volume
        alerts.append(
            Alert(
                node=state.node,
                level="авария",
                rule="возможная утечка",
                text=(
                    f"расход {state.volume_m3:.2f} м³ за окно — в {ratio:.1f} раза больше обычных "
                    f"{state.baseline_volume:.2f} м³"
                ),
                window_start=state.window_start,
            )
        )
    if state.pressure_min < MIN_PRESSURE_BAR:
        alerts.append(
            Alert(
                node=state.node,
                level="авария",
                rule="низкое давление",
                text=f"давление упало до {state.pressure_min:.2f} бар (порог {MIN_PRESSURE_BAR:.1f})",
                window_start=state.window_start,
            )
        )
    return alerts


def _hhmm(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%H:%M")
