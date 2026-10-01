from __future__ import annotations

from pathlib import Path

import pytest
from conftest import reading, series

from teplo.broker import Broker
from teplo.stream import Pipeline
from teplo.warehouse import Warehouse


@pytest.fixture
def pipeline(tmp_path: Path, warehouse: Warehouse) -> Pipeline:
    return Pipeline(Broker(tmp_path / "очередь", partitions=2), warehouse)


def test_batch_goes_from_queue_to_mart(pipeline: Pipeline) -> None:
    pipeline.ingest(series(count=20, step_minutes=0.5))
    result = pipeline.run_once()

    assert (result.events_read, result.stored, result.duplicates, result.broken) == (20, 20, 0, 0)
    assert result.windows == 2
    assert pipeline.warehouse.totals()["показаний"] == 20
    assert sum(pipeline.lag().values()) == 0


def test_same_batch_twice_does_not_double_the_mart(pipeline: Pipeline) -> None:
    batch = series(count=10)
    pipeline.ingest(batch)
    pipeline.run_once()
    energy = pipeline.warehouse.totals()["энергия"]

    pipeline.ingest(batch)  # источник прислал ту же пачку ещё раз
    result = pipeline.run_once()

    assert (result.stored, result.duplicates) == (0, 10)
    assert pipeline.warehouse.totals()["энергия"] == pytest.approx(energy)


def test_duplicate_inside_one_batch_does_not_double_the_mart(pipeline: Pipeline) -> None:
    batch = series(count=10)
    pipeline.ingest([*batch, batch[3], batch[7]])
    result = pipeline.run_once()

    assert (result.events_read, result.stored, result.duplicates) == (12, 10, 2)
    assert pipeline.warehouse.windows()[0].readings == 10


def test_rereading_after_a_crash_is_safe(pipeline: Pipeline) -> None:
    """Падение между записью в витрину и коммитом смещения: пачка перечитывается целиком."""
    pipeline.ingest(series(count=10))
    pipeline.run_once()
    energy = pipeline.warehouse.totals()["энергия"]

    restarted = Pipeline(pipeline.broker, pipeline.warehouse, group="после-сбоя")
    result = restarted.run_once()

    assert (result.events_read, result.stored, result.duplicates) == (10, 0, 10)
    assert pipeline.warehouse.totals()["энергия"] == pytest.approx(energy)


def test_broken_event_does_not_stop_the_stream(pipeline: Pipeline) -> None:
    pipeline.broker.publish("ИТП-1:0", {"node": "ИТП-1"})  # событие без времени и показаний
    pipeline.ingest(series(count=5))
    result = pipeline.run_once()

    assert result.broken == 1
    assert result.stored == 5
    assert "смещение" in result.problems[0]
    assert sum(pipeline.lag().values()) == 0, "битое событие не должно навсегда застрять в очереди"


def test_late_event_is_added_to_a_closed_window(pipeline: Pipeline) -> None:
    pipeline.ingest(series(count=30, step_minutes=0.5))
    pipeline.run_once()

    pipeline.ingest([reading(seq=99, minutes=1.0)])
    result = pipeline.run_once()

    assert result.late == 1
    first = pipeline.warehouse.windows()[0]
    assert first.readings == 11
    assert first.late == 1


def test_run_until_empty_takes_everything(pipeline: Pipeline) -> None:
    pipeline.ingest(series(count=40, step_minutes=0.25))
    results = pipeline.run_until_empty(limit=6)

    assert sum(item.stored for item in results) == 40
    assert sum(pipeline.lag().values()) == 0
    assert len(pipeline.warehouse.runs(limit=50)) == len(results)


def test_empty_queue_is_a_quiet_no_op(pipeline: Pipeline) -> None:
    result = pipeline.run_once()
    assert result.empty
    assert pipeline.warehouse.runs() == []
