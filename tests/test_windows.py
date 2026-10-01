from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from conftest import START, reading, series

from teplo.windows import Watermark, fold, window_start


def test_window_start_floors_to_five_minutes() -> None:
    assert window_start(datetime(2026, 2, 10, 8, 7, 31, tzinfo=UTC)) == datetime(2026, 2, 10, 8, 5, tzinfo=UTC)
    assert window_start(datetime(2026, 2, 10, 8, 5, tzinfo=UTC)) == datetime(2026, 2, 10, 8, 5, tzinfo=UTC)


def test_readings_fold_into_windows_by_event_time() -> None:
    aggregates = fold(series(count=20, step_minutes=0.5), step_seconds=30.0)
    assert [item.start for item in aggregates] == [START, START + timedelta(minutes=5)]
    assert [item.readings for item in aggregates] == [10, 10]
    assert aggregates[0].avg_supply == pytest.approx(82.0)
    assert aggregates[0].avg_delta == pytest.approx(22.0)
    # 10 показаний по 30 с: 4 м³/ч × 300 с = 0,333 м³, 22 °C перепада.
    assert aggregates[0].volume_m3 == pytest.approx(0.3333, abs=0.001)
    assert aggregates[0].energy_kwh == pytest.approx(8.53, abs=0.05)


def test_nodes_do_not_mix() -> None:
    aggregates = fold(series("ИТП-1", 4) + series("ИТП-2", 4), step_seconds=30.0)
    assert {item.node for item in aggregates} == {"ИТП-1", "ИТП-2"}
    assert all(item.readings == 4 for item in aggregates)


def test_minimum_pressure_is_kept() -> None:
    low = reading(seq=3, minutes=1.5, pressure_bar=3.1)
    aggregates = fold([*series(count=3), low], step_seconds=30.0)
    assert aggregates[0].pressure == pytest.approx(3.1)


def test_watermark_closes_window_after_lateness() -> None:
    mark = Watermark(lateness=120.0)
    mark.observe(START + timedelta(minutes=4))
    assert not mark.closed(START + timedelta(minutes=5))

    mark.observe(START + timedelta(minutes=7, seconds=1))
    assert mark.closed(START + timedelta(minutes=5))


def test_late_reading_is_counted_not_dropped() -> None:
    mark = Watermark(lateness=120.0)
    fold(series(count=30, step_minutes=0.5), step_seconds=30.0, watermark=mark)  # поток ушёл до 15-й минуты

    late = reading(seq=99, minutes=1.0)
    aggregates = fold([late], step_seconds=30.0, watermark=mark)
    assert aggregates[0].start == START
    assert aggregates[0].late == 1, "опоздавшее показание должно попасть в своё окно и быть отмечено"


def test_watermark_snapshot_does_not_move() -> None:
    mark = Watermark(lateness=60.0)
    mark.observe(START)
    copy = mark.snapshot()
    mark.observe(START + timedelta(hours=1))
    assert copy.seen == START
