"""Мини-батч: прочитать пачку из очереди, разобрать, сложить в окна, обновить витрину, сдвинуть смещение.

Порядок шагов — главное, что здесь есть. Сначала запись в хранилище, и только потом коммит смещения:
если процесс упадёт между ними, пачка перечитается, а повторы отсечёт первичный ключ сырого слоя.
Обратный порядок (сначала коммит) терял бы данные молча.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass

from teplo.broker import Broker, Record
from teplo.events import EventError, Reading
from teplo.source import STEP_SECONDS
from teplo.warehouse import Warehouse
from teplo.windows import LATENESS, WINDOW, Watermark, fold

DEFAULT_GROUP = "teplo"
DEFAULT_BATCH = 500


@dataclass(frozen=True, slots=True)
class BatchResult:
    """Что случилось за один проход."""

    events_read: int = 0
    stored: int = 0
    duplicates: int = 0
    broken: int = 0
    windows: int = 0
    late: int = 0
    seconds: float = 0.0
    problems: tuple[str, ...] = ()

    @property
    def empty(self) -> bool:
        return self.events_read == 0


class Pipeline:
    """Связка «очередь → окна → витрина»."""

    def __init__(
        self,
        broker: Broker,
        warehouse: Warehouse,
        *,
        group: str = DEFAULT_GROUP,
        step_seconds: float = STEP_SECONDS,
        window: float = WINDOW,
        lateness: float = LATENESS,
    ) -> None:
        self.broker = broker
        self.warehouse = warehouse
        self.group = group
        self.step_seconds = step_seconds
        self.window = window
        self.lateness = lateness

    def ingest(self, readings: Sequence[Reading]) -> int:
        """Положить показания в очередь: ключ — «узел:номер», по нему считается партиция."""
        self.broker.publish_many([(reading.key, reading.to_json()) for reading in readings])
        return len(readings)

    def run_once(self, limit: int = DEFAULT_BATCH) -> BatchResult:
        started = time.monotonic()
        records = self.broker.read(self.group, limit)
        if not records:
            return BatchResult()

        readings, problems = self._parse(records)
        fresh = self.warehouse.store_readings(readings)
        mark = Watermark(lateness=self.lateness, seen=self.warehouse.watermark())
        aggregates = fold(fresh, step_seconds=self.step_seconds, size=self.window, watermark=mark)
        self.warehouse.apply_windows(aggregates)
        self.warehouse.set_watermark(mark.seen)
        self.broker.commit(self.group, records)

        seconds = time.monotonic() - started
        result = BatchResult(
            events_read=len(records),
            stored=len(fresh),
            duplicates=len(readings) - len(fresh),
            broken=len(problems),
            windows=len(aggregates),
            late=sum(item.late for item in aggregates),
            seconds=seconds,
            problems=tuple(problems[:5]),
        )
        self.warehouse.record_run(
            events_read=result.events_read,
            events_stored=result.stored,
            duplicates=result.duplicates,
            windows_touched=result.windows,
            late=result.late,
            seconds=seconds,
        )
        return result

    def run_until_empty(self, limit: int = DEFAULT_BATCH, *, max_batches: int = 1000) -> list[BatchResult]:
        results: list[BatchResult] = []
        for _ in range(max_batches):
            result = self.run_once(limit)
            if result.empty:
                break
            results.append(result)
        return results

    def lag(self) -> dict[int, int]:
        return self.broker.lag(self.group)

    def _parse(self, records: Sequence[Record]) -> tuple[list[Reading], list[str]]:
        """Битое событие не останавливает поток: оно считается, попадает в отчёт и пропускается.

        Одно неожиданное поле не должно ронять разбор всей пятиминутки.
        """
        readings: list[Reading] = []
        problems: list[str] = []
        for record in records:
            try:
                readings.append(Reading.from_json(record.value))
            except EventError as exc:
                problems.append(f"партиция {record.partition}, смещение {record.offset}: {exc}")
        return readings, problems
