from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from teplo.events import Reading
from teplo.warehouse import Warehouse

START = datetime(2026, 2, 10, 8, 0, tzinfo=UTC)


def reading(node: str = "ИТП-1", seq: int = 0, *, minutes: float = 0.0, **changes: float) -> Reading:
    values: dict[str, float] = {"supply_c": 82.0, "return_c": 60.0, "flow_m3h": 4.0, "pressure_bar": 5.5}
    values.update(changes)
    return Reading(
        node=node,
        seq=seq,
        measured_at=START + timedelta(minutes=minutes),
        supply_c=values["supply_c"],
        return_c=values["return_c"],
        flow_m3h=values["flow_m3h"],
        pressure_bar=values["pressure_bar"],
    )


def series(node: str = "ИТП-1", count: int = 10, *, step_minutes: float = 0.5, **changes: float) -> list[Reading]:
    return [reading(node, seq, minutes=seq * step_minutes, **changes) for seq in range(count)]


@pytest.fixture
def warehouse() -> Iterator[Warehouse]:
    store = Warehouse(":memory:")
    yield store
    store.close()
