"""Демонстрация целиком: час телеметрии пяти узлов, шесть мини-батчей, отчёт и страница.

Показывает всё, ради чего конвейер написан: повторы отсекаются, опоздавшие показания досчитываются
в уже закрытые окна, а пять правил находят утечку, перетоп, недотоп и пропавшую связь.
"""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from teplo import alerts as alerts_module
from teplo import report as report_module
from teplo import source as source_module
from teplo.broker import Broker
from teplo.meteo import Meteo, MeteoError, Outside
from teplo.stream import BatchResult, Pipeline
from teplo.warehouse import Warehouse
from teplo.windows import window_start

BATCHES = 6
MAX_DELAY = 250
"""Опоздание в четверть тысячи событий — около десяти минут по времени измерения: больше, чем окно
плюс ожидание, поэтому в демонстрации видно, как опоздавшее показание досчитывается в закрытое окно."""


@dataclass(frozen=True, slots=True)
class DemoResult:
    root: Path
    batches: list[BatchResult]
    text: str
    dashboard: Path


def run(
    *,
    root: Path | None = None,
    minutes: float = 60.0,
    offline: bool = False,
    dashboard: Path | None = None,
) -> DemoResult:
    base = Path(root) if root else Path(tempfile.mkdtemp(prefix="teplo-demo-"))
    base.mkdir(parents=True, exist_ok=True)
    _clean(base)
    outside = _outside(base, offline=offline)

    # Демонстрация заканчивается на границе окна: так последнее окно полное, а не обрезанное на полуслове.
    now = window_start(datetime.now(UTC))
    readings = source_module.generate(
        source_module.DEMO_NODES,
        start=now - timedelta(minutes=minutes),
        minutes=minutes,
        outside_c=outside.temperature_c if outside else -5.0,
    )
    arrivals = source_module.delivery(readings, max_delay=MAX_DELAY)

    broker = Broker(base / "очередь")
    warehouse = Warehouse(base / "витрина.duckdb")
    pipeline = Pipeline(broker, warehouse)

    batches: list[BatchResult] = []
    chunk = max(len(arrivals) // BATCHES, 1)
    for start in range(0, len(arrivals), chunk):
        pipeline.ingest(arrivals[start : start + chunk])
        result = pipeline.run_once(limit=chunk * 2)
        if not result.empty:
            batches.append(result)
    batches.extend(pipeline.run_until_empty())

    states = warehouse.states()
    found = alerts_module.check(states, now=now, outside=outside)
    windows = warehouse.windows(limit=len(source_module.DEMO_NODES) * 12)
    totals = warehouse.totals()

    page = Path(dashboard) if dashboard else base / "дашборд.html"
    page.write_text(
        report_module.dashboard(
            states=states, alerts=found, windows=windows, outside=outside, totals=totals, generated_at=now
        ),
        "utf-8",
    )

    text = "\n".join(
        [
            f"Телеметрия: {len(readings)} показаний, в очередь пришло {len(arrivals)} "
            f"(повторы и опоздания — часть сценария).",
            "",
            report_module.summary(
                states=states, alerts=found, totals=totals, outside=outside, runs=warehouse.runs(limit=BATCHES + 2)
            ),
            "",
            f"Страница отчёта: {page}",
            f"Файлы демонстрации: {base}",
        ]
    )
    warehouse.close()
    return DemoResult(root=base, batches=batches, text=text, dashboard=page)


def _clean(base: Path) -> None:
    """Демонстрация всегда начинается с чистого листа.

    Иначе второй запуск в той же папке читает очередь и витрину от первого, и весь отчёт получается
    про прошлый прогон: «прочитано 96, записано 0, повторов 96». Один раз я на этом и попался.
    """
    shutil.rmtree(base / "очередь", ignore_errors=True)
    (base / "витрина.duckdb").unlink(missing_ok=True)


def _outside(base: Path, *, offline: bool) -> Outside | None:
    if offline:
        return None
    try:
        return Meteo(cache=base / "погода.json").current()
    except MeteoError:
        return None
