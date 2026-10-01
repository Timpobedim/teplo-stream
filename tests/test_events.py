from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from conftest import reading

from teplo.events import EventError, Reading


def test_round_trip_keeps_values() -> None:
    original = reading("ИТП-Ленина-14", 7)
    restored = Reading.from_json(original.to_json())
    assert restored == original
    assert restored.key == "ИТП-Ленина-14:7"


def test_energy_counts_only_useful_heat() -> None:
    warm = reading(supply_c=82.0, return_c=62.0, flow_m3h=4.0)
    # 1,163 кВт·ч на кубометр на градус: 4 м³/ч × 30 с × 20 °C.
    assert warm.energy_kwh(30.0) == pytest.approx(0.7753, abs=0.001)
    assert warm.volume_m3(30.0) == pytest.approx(0.0333, abs=0.001)

    broken = reading(supply_c=60.0, return_c=70.0)
    assert broken.delta_c == -10.0
    assert broken.energy_kwh(30.0) == 0.0


def test_time_is_stored_in_utc_without_zone() -> None:
    moscow = timezone(timedelta(hours=3))
    local = Reading(
        node="ИТП-1",
        seq=1,
        measured_at=datetime(2026, 2, 10, 11, 0, tzinfo=moscow),
        supply_c=80.0,
        return_c=60.0,
        flow_m3h=4.0,
        pressure_bar=5.5,
    )
    stored = local.row()[2]
    # в столбце TIMESTAMP время лежит без пояса — так и сравниваем
    assert stored == datetime(2026, 2, 10, 8, 0)  # noqa: DTZ001
    assert isinstance(stored, datetime)
    assert stored.tzinfo is None


def test_zone_is_restored_on_reading() -> None:
    moscow = timezone(timedelta(hours=3))
    payload = reading().to_json()
    payload["measured_at"] = datetime(2026, 2, 10, 11, 0, tzinfo=moscow).isoformat()
    assert Reading.from_json(payload).measured_at == datetime(2026, 2, 10, 8, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("broken", "message"),
    [
        ("не объект", "объектом JSON"),
        ({"seq": 1}, "нет узла"),
        ({"node": "ИТП-1", "seq": -1}, "не меньше нуля"),
        ({"node": "ИТП-1", "seq": True}, "не меньше нуля"),
        ({"node": "ИТП-1", "seq": 1, "measured_at": 17}, "строкой в формате ISO"),
        ({"node": "ИТП-1", "seq": 1, "measured_at": "вчера"}, "не разобрать"),
    ],
)
def test_mistakes_are_explained(broken: object, message: str) -> None:
    with pytest.raises(EventError, match=message):
        Reading.from_json(broken)


def test_time_without_zone_is_refused() -> None:
    payload = reading().to_json()
    payload["measured_at"] = "2026-02-10T08:00:00"
    with pytest.raises(EventError, match="без часового пояса"):
        Reading.from_json(payload)


def test_values_are_checked_against_limits() -> None:
    payload = reading().to_json()
    payload["supply_c"] = 900.0
    with pytest.raises(EventError, match="вне диапазона"):
        Reading.from_json(payload)

    payload = reading().to_json()
    payload["flow_m3h"] = "много"
    with pytest.raises(EventError, match="«flow_m3h» — число"):
        Reading.from_json(payload)
