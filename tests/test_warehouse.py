from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from conftest import START, reading, series

from teplo.events import Reading
from teplo.warehouse import Warehouse
from teplo.windows import fold


def test_duplicates_are_cut_off_by_the_key(warehouse: Warehouse) -> None:
    batch = series(count=10)
    assert len(warehouse.store_readings(batch)) == 10
    assert warehouse.store_readings(batch) == [], "второй раз те же показания записываться не должны"
    assert warehouse.totals()["строк в сыром слое"] == 10


def test_duplicate_inside_one_batch_is_stored_once(warehouse: Warehouse) -> None:
    """Повтор внутри одной пачки — не то же самое, что повтор между пачками, и ловится отдельно."""
    batch = series(count=5)
    fresh = warehouse.store_readings([*batch, batch[2], batch[4]])
    assert [item.seq for item in fresh] == [0, 1, 2, 3, 4]
    assert warehouse.totals()["строк в сыром слое"] == 5


def test_only_new_readings_come_back(warehouse: Warehouse) -> None:
    warehouse.store_readings(series(count=5))
    mixed = series(count=8)
    fresh = warehouse.store_readings(mixed)
    assert [item.seq for item in fresh] == [5, 6, 7]


def test_window_is_updated_not_overwritten(warehouse: Warehouse) -> None:
    first = [reading(seq=number, minutes=number * 0.5) for number in range(5)]
    second = [reading(seq=number, minutes=number * 0.5) for number in range(5, 10)]
    warehouse.apply_windows(fold(first, step_seconds=30.0))
    warehouse.apply_windows(fold(second, step_seconds=30.0))

    rows = warehouse.windows()
    assert len(rows) == 1, "обе половины относятся к одному окну"
    assert rows[0].readings == 10
    assert rows[0].avg_supply == pytest.approx(82.0)
    assert rows[0].avg_delta == pytest.approx(22.0)


def test_state_compares_last_window_with_the_previous_ones(warehouse: Warehouse) -> None:
    usual = series(count=50, step_minutes=0.5)
    heavy = [reading(seq=number, minutes=number * 0.5, flow_m3h=8.0) for number in range(50, 60)]
    warehouse.apply_windows(fold([*usual, *heavy], step_seconds=30.0))

    state = warehouse.states()[0]
    assert state.windows == 6
    assert state.window_start == START + timedelta(minutes=25)
    assert state.volume_m3 == pytest.approx(state.baseline_volume * 2, rel=0.05)
    assert state.avg_delta == pytest.approx(22.0)


def test_time_comes_back_in_utc(warehouse: Warehouse) -> None:
    moscow = timezone(timedelta(hours=3))
    local = Reading(
        node="ИТП-1",
        seq=1,
        measured_at=datetime(2026, 2, 10, 11, 2, tzinfo=moscow),
        supply_c=80.0,
        return_c=60.0,
        flow_m3h=4.0,
        pressure_bar=5.5,
    )
    warehouse.store_readings([local])
    warehouse.apply_windows(fold([local], step_seconds=30.0))
    assert warehouse.windows()[0].window_start == datetime(2026, 2, 10, 8, 0, tzinfo=UTC)


def test_watermark_is_remembered(warehouse: Warehouse) -> None:
    assert warehouse.watermark() is None
    warehouse.set_watermark(START)
    assert warehouse.watermark() == START
    warehouse.set_watermark(None)
    assert warehouse.watermark() == START


def test_runs_are_written_to_the_journal(warehouse: Warehouse) -> None:
    warehouse.record_run(events_read=10, events_stored=8, duplicates=2, windows_touched=2, late=0, seconds=0.12)
    warehouse.record_run(events_read=4, events_stored=4, duplicates=0, windows_touched=1, late=1, seconds=0.05)
    runs = warehouse.runs()
    assert [run.run_id for run in runs] == [1, 2]
    assert runs[0].duplicates == 2
    assert runs[1].late == 1
    assert runs[1].started_at.tzinfo == UTC


def test_mart_survives_reopening(tmp_path: Path) -> None:
    path = tmp_path / "витрина.duckdb"
    with Warehouse(path) as store:
        store.store_readings(series(count=4))
        store.apply_windows(fold(series(count=4), step_seconds=30.0))
    with Warehouse(path) as store:
        assert store.totals()["строк в сыром слое"] == 4
        assert store.states()[0].readings == 4
