"""Командная строка: demo, produce, run, report, alerts, meteo."""

from __future__ import annotations

import argparse
import io
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from teplo import alerts as alerts_module
from teplo import demo as demo_module
from teplo import report as report_module
from teplo import source as source_module
from teplo.broker import Broker, BrokerError
from teplo.events import EventError
from teplo.meteo import MOSCOW, Meteo, MeteoError, Outside, norm_supply_c
from teplo.stream import Pipeline
from teplo.warehouse import Warehouse, WarehouseError

QUEUE = "очередь"
MART = "витрина.duckdb"
WEATHER = "погода.json"


def main(argv: Sequence[str] | None = None) -> int:
    _utf8_console()
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except (BrokerError, WarehouseError, EventError, MeteoError, ValueError) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2


def _utf8_console() -> None:
    """В стандартной консоли Windows кодировка cp866, и «м³» в ней не печатается.

    Без этого отчёт падал бы с UnicodeEncodeError на первом же кубометре — проверено на этой машине.
    """
    for stream in (sys.stdout, sys.stderr):
        if isinstance(stream, io.TextIOWrapper) and stream.encoding.lower() not in {"utf-8", "utf8"}:
            stream.reconfigure(encoding="utf-8", errors="replace")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="teplo", description="Потоковая обработка телеметрии теплосети")
    commands = parser.add_subparsers(dest="command", required=True)

    demo = commands.add_parser("demo", help="показать всё сразу: поток, витрина, аварии, страница")
    demo.add_argument("--root", type=Path, help="куда положить файлы (по умолчанию — временная папка)")
    demo.add_argument("--minutes", type=float, default=60.0, help="сколько минут телеметрии сгенерировать")
    demo.add_argument("--offline", action="store_true", help="не ходить в сеть за наружной температурой")
    demo.add_argument("--html", type=Path, help="путь для страницы отчёта")
    demo.set_defaults(handler=_demo)

    produce = commands.add_parser("produce", help="сгенерировать телеметрию и положить её в очередь")
    produce.add_argument("--root", type=Path, required=True)
    produce.add_argument("--minutes", type=float, default=60.0)
    produce.add_argument("--offline", action="store_true")
    produce.set_defaults(handler=_produce)

    run = commands.add_parser("run", help="обработать очередь мини-батчами")
    run.add_argument("--root", type=Path, required=True)
    run.add_argument("--batch", type=int, default=500, help="сколько событий брать за проход")
    run.add_argument("--once", action="store_true", help="один проход вместо «пока есть что читать»")
    run.set_defaults(handler=_run)

    report = commands.add_parser("report", help="отчёт по витрине")
    report.add_argument("--root", type=Path, required=True)
    report.add_argument("--html", type=Path, help="дополнительно сохранить страницу")
    report.add_argument("--offline", action="store_true")
    report.set_defaults(handler=_report)

    alerts = commands.add_parser("alerts", help="только аварии и предупреждения")
    alerts.add_argument("--root", type=Path, required=True)
    alerts.add_argument("--offline", action="store_true")
    alerts.set_defaults(handler=_alerts)

    meteo = commands.add_parser("meteo", help="проверить источник наружной температуры")
    meteo.add_argument("--lat", type=float, default=None)
    meteo.add_argument("--lon", type=float, default=None)
    meteo.set_defaults(handler=_meteo)
    return parser


def _demo(args: argparse.Namespace) -> int:
    result = demo_module.run(root=args.root, minutes=args.minutes, offline=args.offline, dashboard=args.html)
    print(result.text)
    return 0


def _produce(args: argparse.Namespace) -> int:
    root = _prepare(args.root)
    outside = _weather(root, offline=args.offline)
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    readings = source_module.generate(
        source_module.DEMO_NODES,
        start=now - timedelta(minutes=args.minutes),
        minutes=args.minutes,
        outside_c=outside.temperature_c if outside else -5.0,
    )
    arrivals = source_module.delivery(readings)
    broker = Broker(root / QUEUE)
    broker.publish_many([(reading.key, reading.to_json()) for reading in arrivals])
    print(f"В очередь положено {len(arrivals)} событий ({len(readings)} показаний плюс повторы).")
    print(f"Очередь: {root / QUEUE}")
    return 0


def _run(args: argparse.Namespace) -> int:
    root = _prepare(args.root)
    broker = Broker(root / QUEUE)
    with Warehouse(root / MART) as warehouse:
        pipeline = Pipeline(broker, warehouse)
        results = [pipeline.run_once(args.batch)] if args.once else pipeline.run_until_empty(args.batch)
        results = [item for item in results if not item.empty]
        if not results:
            print("В очереди нечего обрабатывать.")
            return 0
        for number, result in enumerate(results, start=1):
            print(
                f"Проход {number}: прочитано {result.events_read}, записано {result.stored}, "
                f"повторов {result.duplicates}, окон {result.windows}, опоздавших {result.late}, "
                f"за {result.seconds:.2f} с"
            )
            for problem in result.problems:
                print(f"  битое событие — {problem}")
        print("Остаток в очереди: " + ", ".join(f"партиция {p}: {n}" for p, n in sorted(pipeline.lag().items())))
    return 0


def _report(args: argparse.Namespace) -> int:
    root = _prepare(args.root)
    outside = _weather(root, offline=args.offline)
    with Warehouse(root / MART) as warehouse:
        states = warehouse.states()
        if not states:
            print("Витрина пуста: сначала «teplo produce», затем «teplo run».")
            return 0
        found = alerts_module.check(states, outside=outside)
        totals = warehouse.totals()
        print(
            report_module.summary(
                states=states, alerts=found, totals=totals, outside=outside, runs=warehouse.runs(limit=8)
            )
        )
        if args.html:
            page = report_module.dashboard(
                states=states,
                alerts=found,
                windows=warehouse.windows(limit=len(states) * 12),
                outside=outside,
                totals=totals,
            )
            Path(args.html).write_text(page, "utf-8")
            print(f"\nСтраница отчёта: {args.html}")
    return 0


def _alerts(args: argparse.Namespace) -> int:
    root = _prepare(args.root)
    outside = _weather(root, offline=args.offline)
    with Warehouse(root / MART) as warehouse:
        found = alerts_module.check(warehouse.states(), outside=outside)
    print(report_module.alerts_block(found))
    return 1 if any(alert.level == "авария" for alert in found) else 0


def _meteo(args: argparse.Namespace) -> int:
    meteo = Meteo(
        latitude=args.lat if args.lat is not None else MOSCOW[0],
        longitude=args.lon if args.lon is not None else MOSCOW[1],
    )
    outside = meteo.current()
    print(
        f"На улице {outside.temperature_c:.1f} °C ({outside.source}, {outside.measured_at:%d.%m %H:%M} UTC). "
        f"Норма подачи по графику — {norm_supply_c(outside.temperature_c):.1f} °C."
    )
    return 0


def _prepare(root: Path) -> Path:
    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _weather(root: Path, *, offline: bool) -> Outside | None:
    if offline:
        return None
    try:
        return Meteo(cache=root / WEATHER).current()
    except MeteoError:
        print("Наружная температура недоступна — правило «недотоп» отключено.", file=sys.stderr)
        return None


if __name__ == "__main__":
    raise SystemExit(main())
