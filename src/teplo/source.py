"""Эмулятор узлов учёта: ровно те неприятности, ради которых конвейер и пишется.

Настоящую телеметрию теплосети в открытый доступ никто не выкладывает, поэтому поток создаётся здесь:
с повторами, опозданиями, обрывом связи, утечкой и перетопом. Зато поток воспроизводим — по одному
и тому же зерну получается один и тот же набор событий, и проверки не зависят от случайности.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from teplo.events import Reading
from teplo.meteo import norm_supply_c

BEHAVIOURS: Final = ("норма", "утечка", "перетоп", "недотоп", "обрыв")
STEP_SECONDS: Final = 30.0


@dataclass(frozen=True, slots=True)
class Node:
    """Узел учёта и то, как он себя ведёт."""

    name: str
    behaviour: str = "норма"
    flow_m3h: float = 4.0
    delta_c: float = 22.0


DEMO_NODES: Final = (
    Node("ИТП-Ленина-14", "норма", flow_m3h=4.2, delta_c=23.0),
    Node("ИТП-Садовая-3", "утечка", flow_m3h=3.6, delta_c=21.0),
    Node("ИТП-Мира-27", "перетоп", flow_m3h=5.1, delta_c=7.0),
    Node("ИТП-Гагарина-5", "недотоп", flow_m3h=4.4, delta_c=20.0),
    Node("ИТП-Заречная-8", "обрыв", flow_m3h=3.9, delta_c=22.5),
)


def generate(
    nodes: Sequence[Node],
    *,
    start: datetime,
    minutes: float,
    step_seconds: float = STEP_SECONDS,
    outside_c: float = -5.0,
    seed: int = 42,
) -> list[Reading]:
    """Показания всех узлов за указанный отрезок времени, отсортированные по времени измерения."""
    if start.tzinfo is None:
        raise ValueError("Начало отрезка — момент времени с часовым поясом")
    for node in nodes:
        if node.behaviour not in BEHAVIOURS:
            raise ValueError(f"Узел «{node.name}»: неизвестное поведение «{node.behaviour}»")
    rng = random.Random(seed)
    steps = max(int(minutes * 60 / step_seconds), 1)
    supply_norm = norm_supply_c(outside_c)
    readings: list[Reading] = []
    for node in nodes:
        for step in range(steps):
            share = step / steps
            if node.behaviour == "обрыв" and share >= 0.65:
                continue  # связь с узлом пропала и больше не восстановилась
            leak = max(share - 0.5, 0.0) * 2 if node.behaviour == "утечка" else 0.0
            flow = node.flow_m3h * (1.0 + 0.9 * leak)
            pressure = 5.6 - 1.7 * leak
            supply = supply_norm + rng.uniform(-1.2, 1.2) - (9.0 if node.behaviour == "недотоп" else 0.0)
            readings.append(
                Reading(
                    node=node.name,
                    seq=step,
                    measured_at=start + timedelta(seconds=step * step_seconds),
                    supply_c=round(supply, 2),
                    return_c=round(supply - node.delta_c + rng.uniform(-0.8, 0.8), 2),
                    flow_m3h=round(flow + rng.uniform(-0.15, 0.15), 3),
                    pressure_bar=round(pressure + rng.uniform(-0.05, 0.05), 3),
                )
            )
    readings.sort(key=lambda item: (item.measured_at, item.node))
    return readings


def delivery(
    readings: Sequence[Reading],
    *,
    seed: int = 42,
    late_share: float = 0.05,
    duplicate_share: float = 0.03,
    max_delay: int = 25,
) -> list[Reading]:
    """Порядок, в котором показания реально доходят до очереди: часть опаздывает, часть дублируется."""
    rng = random.Random(seed)
    ordered: list[tuple[int, int, Reading]] = []
    for position, reading in enumerate(readings):
        delay = rng.randint(5, max_delay) if rng.random() < late_share else 0
        ordered.append((position + delay, position, reading))
    ordered.sort(key=lambda item: (item[0], item[1]))
    arrived: list[Reading] = []
    for _, _, reading in ordered:
        arrived.append(reading)
        if rng.random() < duplicate_share:
            arrived.append(reading)
    return arrived
