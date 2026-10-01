"""Хранилище: сырой слой, витрина по окнам и состояние конвейера — один файл DuckDB.

Колоночная база, а не PostgreSQL: запросы к витрине аналитические («сколько тепла по узлам за сутки»),
а не по одной строке. Дубликаты не отсеиваются чтением всей таблицы в память и анти-join: у сырого
слоя есть первичный ключ (узел, номер показания), вставка идёт через `ON CONFLICT DO NOTHING`, а
`RETURNING` возвращает ровно те строки, которых раньше не было, — только они и попадают в витрину.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from teplo.events import COLUMNS, Reading
from teplo.windows import Aggregate

RAW_COLUMNS = ", ".join(f"{name} {kind}" for name, kind in COLUMNS)
PLACEHOLDERS = ", ".join("?" for _ in COLUMNS)


class WarehouseError(RuntimeError):
    """Хранилище недоступно или запрос к нему не выполнился."""


@dataclass(frozen=True, slots=True)
class NodeState:
    """Последнее окно узла плюс то, с чем это окно сравнивать."""

    node: str
    window_start: datetime
    window_end: datetime
    readings: int
    energy_kwh: float
    volume_m3: float
    avg_supply: float
    avg_return: float
    pressure_min: float
    late: int
    baseline_volume: float
    windows: int
    total_energy: float

    @property
    def avg_delta(self) -> float:
        return self.avg_supply - self.avg_return


@dataclass(frozen=True, slots=True)
class WindowRow:
    node: str
    window_start: datetime
    window_end: datetime
    readings: int
    energy_kwh: float
    volume_m3: float
    avg_supply: float
    avg_return: float
    pressure_min: float
    late: int

    @property
    def avg_delta(self) -> float:
        return self.avg_supply - self.avg_return


@dataclass(frozen=True, slots=True)
class Run:
    run_id: int
    started_at: datetime
    events_read: int
    events_stored: int
    duplicates: int
    windows_touched: int
    late: int
    seconds: float


class Warehouse:
    """Сырой слой, витрина и состояние конвейера."""

    def __init__(self, path: Path | str = ":memory:") -> None:
        try:
            self.connection = duckdb.connect(str(path))
        except duckdb.Error as exc:
            raise WarehouseError(f"Не удалось открыть хранилище {path}: {exc}") from exc
        self._prepare()

    def _prepare(self) -> None:
        self.execute(f"CREATE TABLE IF NOT EXISTS raw_readings ({RAW_COLUMNS}, PRIMARY KEY (node, seq))")
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS mart_window (
                node VARCHAR,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                readings INTEGER,
                energy_kwh DOUBLE,
                volume_m3 DOUBLE,
                supply_sum DOUBLE,
                return_sum DOUBLE,
                pressure_min DOUBLE,
                late INTEGER,
                updated_at TIMESTAMP,
                PRIMARY KEY (node, window_start)
            )
            """
        )
        self.execute("CREATE TABLE IF NOT EXISTS pipeline_state (name VARCHAR PRIMARY KEY, value VARCHAR)")
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id BIGINT PRIMARY KEY,
                started_at TIMESTAMP,
                events_read INTEGER,
                events_stored INTEGER,
                duplicates INTEGER,
                windows_touched INTEGER,
                late INTEGER,
                seconds DOUBLE
            )
            """
        )

    # ── сырой слой ──────────────────────────────────────────────────────────────────────────────

    def store_readings(self, readings: Sequence[Reading]) -> list[Reading]:
        """Пишет показания и возвращает только новые: повторы отсекает первичный ключ.

        Витрину досчитывают именно возвращённые строки — поэтому перечитанная после сбоя пачка
        не удваивает потребление. Повторы бывают двух видов, и оба надо поймать: одно и то же
        показание в разных пачках снимает `ON CONFLICT`, а два одинаковых показания внутри одной
        пачки — вот этот словарь: для базы это одна строка, и вернуть её как две было бы обманом.

        Имена столбцов в запросе подставляются из COLUMNS — это схема самого модуля, а не ввод
        снаружи; все значения идут параметрами.
        """
        if not readings:
            return []
        unique: dict[tuple[str, int], Reading] = {}
        for reading in readings:
            unique.setdefault((reading.node, reading.seq), reading)
        self.execute(f"CREATE OR REPLACE TEMP TABLE stage ({RAW_COLUMNS})")
        self.executemany(
            f"INSERT INTO stage VALUES ({PLACEHOLDERS})",  # noqa: S608
            [item.row() for item in unique.values()],
        )
        inserted = self.query("INSERT INTO raw_readings SELECT * FROM stage ON CONFLICT DO NOTHING RETURNING node, seq")
        fresh = {(str(row[0]), int(row[1])) for row in inserted}
        return [item for key, item in unique.items() if key in fresh]

    # ── витрина ─────────────────────────────────────────────────────────────────────────────────

    def apply_windows(self, aggregates: Sequence[Aggregate]) -> int:
        """Досчитывает окна в витрину.

        В витрине хранятся суммы, а не средние: среднее нельзя досчитать, а сумму — можно, поэтому
        опоздавшее показание просто прибавляется к своему окну.
        """
        if not aggregates:
            return 0
        now = datetime.now(UTC).replace(tzinfo=None)
        rows = [
            (
                item.node,
                _naive(item.start),
                _naive(item.end),
                item.readings,
                item.energy_kwh,
                item.volume_m3,
                item.supply_sum,
                item.return_sum,
                item.pressure,
                item.late,
                now,
            )
            for item in aggregates
        ]
        self.executemany(
            """
            INSERT INTO mart_window VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (node, window_start) DO UPDATE SET
                readings = mart_window.readings + excluded.readings,
                energy_kwh = mart_window.energy_kwh + excluded.energy_kwh,
                volume_m3 = mart_window.volume_m3 + excluded.volume_m3,
                supply_sum = mart_window.supply_sum + excluded.supply_sum,
                return_sum = mart_window.return_sum + excluded.return_sum,
                pressure_min = least(mart_window.pressure_min, excluded.pressure_min),
                late = mart_window.late + excluded.late,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        return len(rows)

    def states(self) -> list[NodeState]:
        """По каждому узлу — последнее окно и медиана объёма по предыдущим (для правила «утечка»)."""
        rows = self.query(
            """
            WITH ranked AS (
                SELECT *, row_number() OVER (PARTITION BY node ORDER BY window_start DESC) AS rn
                FROM mart_window
            ),
            baseline AS (
                SELECT node,
                       median(volume_m3) AS baseline_volume,
                       count(*) AS windows,
                       sum(energy_kwh) AS total_energy
                FROM ranked WHERE rn > 1 GROUP BY node
            )
            SELECT r.node, r.window_start, r.window_end, r.readings, r.energy_kwh, r.volume_m3,
                   r.supply_sum / nullif(r.readings, 0), r.return_sum / nullif(r.readings, 0),
                   r.pressure_min, r.late,
                   coalesce(b.baseline_volume, r.volume_m3),
                   coalesce(b.windows, 0) + 1,
                   coalesce(b.total_energy, 0) + r.energy_kwh
            FROM ranked r LEFT JOIN baseline b ON b.node = r.node
            WHERE r.rn = 1
            ORDER BY r.node
            """
        )
        return [
            NodeState(
                node=str(row[0]),
                window_start=_aware(row[1]),
                window_end=_aware(row[2]),
                readings=int(row[3]),
                energy_kwh=float(row[4]),
                volume_m3=float(row[5]),
                avg_supply=float(row[6] or 0.0),
                avg_return=float(row[7] or 0.0),
                pressure_min=float(row[8]),
                late=int(row[9]),
                baseline_volume=float(row[10]),
                windows=int(row[11]),
                total_energy=float(row[12]),
            )
            for row in rows
        ]

    def windows(self, *, node: str | None = None, limit: int = 24) -> list[WindowRow]:
        """Последние окна витрины — по одному узлу или по всем."""
        where = "WHERE node = ?" if node else ""
        parameters: list[object] = [node] if node else []
        parameters.append(limit)
        rows = self.query(
            f"""
            SELECT node, window_start, window_end, readings, energy_kwh, volume_m3,
                   supply_sum / nullif(readings, 0), return_sum / nullif(readings, 0), pressure_min, late
            FROM mart_window {where}
            ORDER BY window_start DESC, node
            LIMIT ?
            """,  # noqa: S608 — «where» собран из литералов модуля, имя узла и предел идут параметрами
            parameters,
        )
        rows.reverse()
        return [
            WindowRow(
                node=str(row[0]),
                window_start=_aware(row[1]),
                window_end=_aware(row[2]),
                readings=int(row[3]),
                energy_kwh=float(row[4]),
                volume_m3=float(row[5]),
                avg_supply=float(row[6] or 0.0),
                avg_return=float(row[7] or 0.0),
                pressure_min=float(row[8]),
                late=int(row[9]),
            )
            for row in rows
        ]

    def totals(self) -> dict[str, float]:
        row = self.query(
            """
            SELECT coalesce(count(*), 0), coalesce(sum(readings), 0), coalesce(sum(energy_kwh), 0.0),
                   coalesce(sum(late), 0), coalesce(count(DISTINCT node), 0)
            FROM mart_window
            """
        )[0]
        raw = self.query("SELECT count(*) FROM raw_readings")[0][0]
        return {
            "окон": float(row[0]),
            "показаний": float(row[1]),
            "энергия": float(row[2]),
            "опоздавших": float(row[3]),
            "узлов": float(row[4]),
            "строк в сыром слое": float(raw),
        }

    # ── состояние конвейера ─────────────────────────────────────────────────────────────────────

    def watermark(self) -> datetime | None:
        rows = self.query("SELECT value FROM pipeline_state WHERE name = 'watermark'")
        if not rows:
            return None
        return datetime.fromisoformat(str(rows[0][0]))

    def set_watermark(self, moment: datetime | None) -> None:
        if moment is None:
            return
        self.execute(
            "INSERT INTO pipeline_state VALUES ('watermark', ?) "
            "ON CONFLICT (name) DO UPDATE SET value = excluded.value",
            [moment.astimezone(UTC).isoformat()],
        )

    def record_run(
        self,
        *,
        events_read: int,
        events_stored: int,
        duplicates: int,
        windows_touched: int,
        late: int,
        seconds: float,
    ) -> int:
        run_id = int(self.query("SELECT coalesce(max(run_id), 0) + 1 FROM runs")[0][0])
        self.execute(
            "INSERT INTO runs VALUES (?,?,?,?,?,?,?,?)",
            [
                run_id,
                datetime.now(UTC).replace(tzinfo=None),
                events_read,
                events_stored,
                duplicates,
                windows_touched,
                late,
                round(seconds, 3),
            ],
        )
        return run_id

    def runs(self, limit: int = 10) -> list[Run]:
        rows = self.query("SELECT * FROM runs ORDER BY run_id DESC LIMIT ?", [limit])
        rows.reverse()
        return [
            Run(
                run_id=int(row[0]),
                started_at=_aware(row[1]),
                events_read=int(row[2]),
                events_stored=int(row[3]),
                duplicates=int(row[4]),
                windows_touched=int(row[5]),
                late=int(row[6]),
                seconds=float(row[7]),
            )
            for row in rows
        ]

    # ── низкий уровень ──────────────────────────────────────────────────────────────────────────

    def execute(self, sql: str, parameters: Sequence[object] | None = None) -> None:
        try:
            self.connection.execute(sql, list(parameters) if parameters else None)
        except duckdb.Error as exc:
            raise WarehouseError(f"Запрос не выполнился: {exc}") from exc

    def executemany(self, sql: str, rows: Sequence[Sequence[object]]) -> None:
        try:
            self.connection.executemany(sql, [list(row) for row in rows])
        except duckdb.Error as exc:
            raise WarehouseError(f"Запрос не выполнился: {exc}") from exc

    def query(self, sql: str, parameters: Sequence[object] | None = None) -> list[tuple[Any, ...]]:
        try:
            result = self.connection.execute(sql, list(parameters) if parameters else None).fetchall()
        except duckdb.Error as exc:
            raise WarehouseError(f"Запрос не выполнился: {exc}") from exc
        return list(result)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> Warehouse:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _naive(moment: datetime) -> datetime:
    """UTC без пояса — так время ложится в TIMESTAMP одинаково на любой машине."""
    return moment.astimezone(UTC).replace(tzinfo=None)


def _aware(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise WarehouseError(f"Ожидалось время, получено {value!r}")
    return value.replace(tzinfo=UTC)
