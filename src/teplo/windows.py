"""Окна по времени события и водяной знак: что делать с показаниями, которые пришли поздно.

Если складывать события в таблицу по порядку прихода, прибор учёта, который полчаса не мог
достучаться до сервера, молча испортит витрину. Поэтому показание попадает в окно по времени измерения, а не по времени
доставки, и опоздавшее показание досчитывается в уже закрытое окно, а не теряется.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import inf

from teplo.events import Reading

WINDOW = 300.0
"""Пять минут — шаг окна."""

LATENESS = 120.0
"""Сколько ждём опоздавшие показания, прежде чем считать окно закрытым."""


def window_start(moment: datetime, size: float = WINDOW) -> datetime:
    """Начало окна, в которое попадает момент времени."""
    return datetime.fromtimestamp(int(moment.timestamp() // size * size), UTC)


@dataclass
class Aggregate:
    """Свёртка показаний одного узла за одно окно."""

    node: str
    start: datetime
    end: datetime
    readings: int = 0
    energy_kwh: float = 0.0
    volume_m3: float = 0.0
    supply_sum: float = 0.0
    return_sum: float = 0.0
    pressure_min: float = inf
    late: int = 0

    def add(self, reading: Reading, step_seconds: float, *, late: bool = False) -> None:
        self.readings += 1
        self.energy_kwh += reading.energy_kwh(step_seconds)
        self.volume_m3 += reading.volume_m3(step_seconds)
        self.supply_sum += reading.supply_c
        self.return_sum += reading.return_c
        self.pressure_min = min(self.pressure_min, reading.pressure_bar)
        if late:
            self.late += 1

    @property
    def avg_supply(self) -> float:
        return self.supply_sum / self.readings if self.readings else 0.0

    @property
    def avg_return(self) -> float:
        return self.return_sum / self.readings if self.readings else 0.0

    @property
    def avg_delta(self) -> float:
        return self.avg_supply - self.avg_return

    @property
    def pressure(self) -> float:
        return 0.0 if self.pressure_min == inf else self.pressure_min


@dataclass
class Watermark:
    """Насколько далеко продвинулось время событий: по нему решается, закрыто ли окно."""

    lateness: float = LATENESS
    seen: datetime | None = None

    def observe(self, moment: datetime) -> None:
        if self.seen is None or moment > self.seen:
            self.seen = moment

    def closed(self, end: datetime) -> bool:
        return self.seen is not None and self.seen >= end + timedelta(seconds=self.lateness)

    def snapshot(self) -> Watermark:
        return Watermark(lateness=self.lateness, seen=self.seen)


def fold(
    readings: list[Reading],
    *,
    step_seconds: float,
    size: float = WINDOW,
    watermark: Watermark | None = None,
) -> list[Aggregate]:
    """Свернуть пачку показаний в окна.

    Опоздавшим считается показание, чьё окно водяной знак закрыл ещё до этой пачки. Такое показание
    не выбрасывается: оно досчитывается в витрину и отмечается отдельным счётчиком, чтобы по отчёту
    было видно, какие окна пересчитывались.
    """
    mark = watermark if watermark is not None else Watermark()
    boundary = mark.snapshot()
    groups: dict[tuple[str, datetime], Aggregate] = {}
    for reading in readings:
        start = window_start(reading.measured_at, size)
        end = start + timedelta(seconds=size)
        key = (reading.node, start)
        aggregate = groups.get(key)
        if aggregate is None:
            aggregate = Aggregate(node=reading.node, start=start, end=end)
            groups[key] = aggregate
        aggregate.add(reading, step_seconds, late=boundary.closed(end))
        mark.observe(reading.measured_at)
    return sorted(groups.values(), key=lambda item: (item.start, item.node))
