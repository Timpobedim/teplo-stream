"""Одно измерение узла учёта — и единственное место, где описана его схема.

Схема события описана один раз, а не в трёх местах (dataclass в продюсере, `StructType` в Spark-джобе
и `CREATE TABLE` в SQL), где расхождение обнаруживалось бы уже в данных. Схема одна: `Reading` и `COLUMNS`
рядом с ним, а хранилище строит DDL из этого же списка.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

WATER_HEAT: Final = 1.163
"""Кубометр воды, остывая на один градус, отдаёт 1,163 кВт·ч — по этому и считается тепло."""

COLUMNS: Final = (
    ("node", "VARCHAR"),
    ("seq", "BIGINT"),
    ("measured_at", "TIMESTAMP"),
    ("supply_c", "DOUBLE"),
    ("return_c", "DOUBLE"),
    ("flow_m3h", "DOUBLE"),
    ("pressure_bar", "DOUBLE"),
)

LIMITS: Final = {
    "supply_c": (-60.0, 200.0),
    "return_c": (-60.0, 200.0),
    "flow_m3h": (0.0, 500.0),
    "pressure_bar": (0.0, 25.0),
}


class EventError(ValueError):
    """Событие не прошло проверку; текст — для человека, который чинит источник."""


@dataclass(frozen=True, slots=True)
class Reading:
    """Показания узла учёта: температуры подачи и обратки, расход и давление."""

    node: str
    seq: int
    measured_at: datetime
    supply_c: float
    return_c: float
    flow_m3h: float
    pressure_bar: float

    @property
    def key(self) -> str:
        """Ключ события: по нему считается партиция и ловятся повторы."""
        return f"{self.node}:{self.seq}"

    @property
    def delta_c(self) -> float:
        """Перепад между подачей и обраткой — сколько тепла дом действительно взял."""
        return self.supply_c - self.return_c

    def energy_kwh(self, seconds: float) -> float:
        """Тепло за интервал в кВт·ч. Отрицательный перепад — это неисправность, а не отрицательное тепло."""
        return WATER_HEAT * self.flow_m3h * (seconds / 3600.0) * max(self.delta_c, 0.0)

    def volume_m3(self, seconds: float) -> float:
        return self.flow_m3h * (seconds / 3600.0)

    def to_json(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "seq": self.seq,
            "measured_at": self.measured_at.astimezone(UTC).isoformat(),
            "supply_c": self.supply_c,
            "return_c": self.return_c,
            "flow_m3h": self.flow_m3h,
            "pressure_bar": self.pressure_bar,
        }

    def row(self) -> tuple[object, ...]:
        """Строка для хранилища — в том же порядке, что и COLUMNS.

        Часовой пояс снимается намеренно: в столбец TIMESTAMP DuckDB кладёт момент, пересчитанный
        в местное время машины, и один и тот же поток на сервере в Москве и на ноутбуке во Владивостоке
        попал бы в разные окна. В хранилище всё время в UTC, пояс возвращается при чтении.
        """
        return (
            self.node,
            self.seq,
            self.measured_at.astimezone(UTC).replace(tzinfo=None),
            self.supply_c,
            self.return_c,
            self.flow_m3h,
            self.pressure_bar,
        )

    @classmethod
    def from_json(cls, raw: object) -> Reading:
        """Разбирает событие из очереди. Всё, что не сходится, — ошибка с понятным текстом."""
        if not isinstance(raw, dict):
            raise EventError("Событие должно быть объектом JSON")
        node = str(raw.get("node", "")).strip()
        if not node:
            raise EventError("У события нет узла («node»)")
        seq = _whole(raw.get("seq"), node, "seq")
        measured_at = _moment(raw.get("measured_at"), node)
        values = {name: _real(raw.get(name), node, name) for name in LIMITS}
        return cls(node=node, seq=seq, measured_at=measured_at, **values)


def _whole(raw: object, node: str, field: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        raise EventError(f"Узел «{node}»: «{field}» — целое число не меньше нуля, получено {raw!r}")
    return raw


def _real(raw: object, node: str, field: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, int | float) or not math.isfinite(float(raw)):
        raise EventError(f"Узел «{node}»: «{field}» — число, получено {raw!r}")
    value = float(raw)
    low, high = LIMITS[field]
    if not low <= value <= high:
        raise EventError(f"Узел «{node}»: «{field}» = {value} вне диапазона {low}…{high}")
    return value


def _moment(raw: object, node: str) -> datetime:
    if not isinstance(raw, str):
        raise EventError(f"Узел «{node}»: «measured_at» — время строкой в формате ISO")
    try:
        moment = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise EventError(f"Узел «{node}»: время «{raw}» не разобрать ({exc})") from exc
    if moment.tzinfo is None:
        raise EventError(f"Узел «{node}»: время «{raw}» без часового пояса — неясно, какой это момент")
    return moment.astimezone(UTC)
