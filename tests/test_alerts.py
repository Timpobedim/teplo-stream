from __future__ import annotations

from datetime import timedelta
from typing import Any

from conftest import START

from teplo.alerts import check
from teplo.meteo import Outside
from teplo.warehouse import NodeState

NOW = START + timedelta(minutes=6)
STREET = Outside(measured_at=NOW, temperature_c=-5.0, source="сеть")


def state(**changes: Any) -> NodeState:
    values: dict[str, Any] = {
        "node": "ИТП-1",
        "window_start": START,
        "window_end": START + timedelta(minutes=5),
        "readings": 10,
        "energy_kwh": 8.5,
        "volume_m3": 0.33,
        "avg_supply": 82.0,
        "avg_return": 60.0,
        "pressure_min": 5.5,
        "late": 0,
        "baseline_volume": 0.33,
        "windows": 6,
        "total_energy": 50.0,
    }
    values.update(changes)
    return NodeState(**values)


def test_healthy_node_is_silent() -> None:
    assert check([state()], now=NOW, outside=STREET) == []


def test_lost_connection_is_an_alarm() -> None:
    lost = state(window_start=START - timedelta(minutes=20), window_end=START - timedelta(minutes=15))
    alerts = check([lost], now=NOW, outside=STREET)
    assert [alert.rule for alert in alerts] == ["нет связи"]
    assert alerts[0].level == "авария"
    assert "мин" in alerts[0].text


def test_underheating_needs_the_street_temperature() -> None:
    cold = state(avg_supply=63.0, avg_return=40.0)
    outside = Outside(measured_at=NOW, temperature_c=-15.0, source="сеть")
    assert [alert.rule for alert in check([cold], now=NOW, outside=outside)] == ["недотоп"]
    assert check([cold], now=NOW) == [], "без наружной температуры правило должно молчать, а не выдумывать"


def test_small_delta_is_a_warning() -> None:
    overheat = state(avg_supply=80.0, avg_return=73.0)
    alerts = check([overheat], now=NOW, outside=STREET)
    assert [alert.rule for alert in alerts] == ["малый перепад"]
    assert alerts[0].level == "внимание"


def test_leak_and_pressure_are_alarms() -> None:
    leak = state(volume_m3=0.8, pressure_min=3.5)
    rules = {alert.rule for alert in check([leak], now=NOW, outside=STREET)}
    assert rules == {"возможная утечка", "низкое давление"}


def test_new_node_is_not_accused_of_leaking() -> None:
    young = state(volume_m3=0.8, windows=2)
    assert check([young], now=NOW, outside=STREET) == []


def test_alarms_come_before_warnings() -> None:
    alerts = check(
        [state(node="Б", avg_supply=80.0, avg_return=73.0), state(node="А", pressure_min=2.0)],
        now=NOW,
        outside=STREET,
    )
    assert [alert.level for alert in alerts] == ["авария", "внимание"]
    assert alerts[0].node == "А"
