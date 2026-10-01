"""Очередь событий на диске: партиции, смещения и коммит группы — то, что обычно делает Kafka.

Поднимать Kafka в Docker ради одного потока телеметрии дорого, поэтому очередь здесь файловая: топик —
папка, партиция — файл, смещение — номер строки. Свойства, ради которых очередь вообще нужна, сохранены:
события разложены по партициям устойчивым хешем ключа, потребитель читает с последнего закоммиченного
смещения, а коммит происходит после записи в хранилище, а не до.
"""

from __future__ import annotations

import json
import os
import zlib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_PARTITIONS = 3


class BrokerError(RuntimeError):
    """Очередь недоступна или повреждена."""


@dataclass(frozen=True, slots=True)
class Record:
    partition: int
    offset: int
    key: str
    value: dict[str, Any]


class Broker:
    """Файловая очередь. Писатель предполагается один — это учебная очередь, а не замена Kafka."""

    def __init__(self, root: Path, partitions: int = DEFAULT_PARTITIONS) -> None:
        if partitions < 1:
            raise BrokerError("Партиций должно быть хотя бы одна")
        self.root = Path(root)
        self.partitions = partitions
        self._sizes: dict[int, int] = {}
        try:
            (self.root / "offsets").mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise BrokerError(f"Не удалось создать очередь в {self.root}: {exc}") from exc

    # ── запись ──────────────────────────────────────────────────────────────────────────────────

    def partition_for(self, key: str) -> int:
        """Устойчивый хеш: встроенный hash() солится при каждом запуске, и события одного узла
        после перезапуска уехали бы в другую партицию."""
        return zlib.crc32(key.encode("utf-8")) % self.partitions

    def publish(self, key: str, value: dict[str, Any]) -> Record:
        return self.publish_many([(key, value)])[0]

    def publish_many(self, items: Sequence[tuple[str, dict[str, Any]]]) -> list[Record]:
        batches: dict[int, list[tuple[str, dict[str, Any]]]] = {}
        for key, value in items:
            batches.setdefault(self.partition_for(key), []).append((key, value))
        written: list[Record] = []
        for partition, batch in sorted(batches.items()):
            offset = self.size(partition)
            lines: list[str] = []
            for key, value in batch:
                lines.append(json.dumps({"key": key, "value": value}, ensure_ascii=False))
                written.append(Record(partition=partition, offset=offset, key=key, value=value))
                offset += 1
            try:
                with self._path(partition).open("a", encoding="utf-8") as handle:
                    handle.write("\n".join(lines) + "\n")
            except OSError as exc:
                raise BrokerError(f"Не удалось записать в партицию {partition}: {exc}") from exc
            self._sizes[partition] = offset
        return written

    # ── чтение ──────────────────────────────────────────────────────────────────────────────────

    def read(self, group: str, limit: int = 500) -> list[Record]:
        """Пачка событий с последнего закоммиченного места. Партиции читаются поровну, чтобы первая
        не съедала всю пачку и остальные не отставали бесконечно."""
        if limit < 1:
            raise BrokerError("Размер пачки — положительное число")
        committed = self.committed(group)
        share = max(1, limit // self.partitions)
        records: list[Record] = []
        for partition in range(self.partitions):
            room = min(share, limit - len(records))
            if room <= 0:
                break
            for taken, record in enumerate(self._scan(partition, committed.get(partition, 0)), start=1):
                records.append(record)
                if taken >= room:
                    break
        return records

    def _scan(self, partition: int, start: int) -> Iterable[Record]:
        path = self._path(partition)
        if not path.exists():
            return
        try:
            with path.open(encoding="utf-8") as handle:
                for offset, line in enumerate(handle):
                    if offset < start or not line.strip():
                        continue
                    yield self._record(partition, offset, line)
        except OSError as exc:
            raise BrokerError(f"Не удалось прочитать партицию {partition}: {exc}") from exc

    def _record(self, partition: int, offset: int, line: str) -> Record:
        try:
            raw = json.loads(line)
            return Record(partition=partition, offset=offset, key=str(raw["key"]), value=dict(raw["value"]))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise BrokerError(f"Партиция {partition}, запись {offset}: строка испорчена ({exc})") from exc

    # ── смещения ────────────────────────────────────────────────────────────────────────────────

    def committed(self, group: str) -> dict[int, int]:
        path = self._group_path(group)
        if not path.exists():
            return {}
        try:
            raw = json.loads(path.read_text("utf-8"))
            return {int(key): int(value) for key, value in raw.items()}
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise BrokerError(f"Смещения группы «{group}» испорчены: {exc}") from exc

    def commit(self, group: str, records: Iterable[Record]) -> dict[int, int]:
        """Коммит вызывается после записи в хранилище: упасть между записью и коммитом безопасно —
        пачка перечитается, а повторы отсеются по ключу события."""
        offsets = self.committed(group)
        for record in records:
            offsets[record.partition] = max(offsets.get(record.partition, 0), record.offset + 1)
        path = self._group_path(group)
        tmp = path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps({str(k): v for k, v in sorted(offsets.items())}, indent=2), "utf-8")
            os.replace(tmp, path)
        except OSError as exc:
            raise BrokerError(f"Не удалось сохранить смещения группы «{group}»: {exc}") from exc
        return offsets

    def lag(self, group: str) -> dict[int, int]:
        """Сколько событий в каждой партиции ещё не обработано."""
        committed = self.committed(group)
        return {partition: self.size(partition) - committed.get(partition, 0) for partition in range(self.partitions)}

    def size(self, partition: int) -> int:
        cached = self._sizes.get(partition)
        if cached is None:
            path = self._path(partition)
            try:
                cached = sum(1 for line in path.open(encoding="utf-8") if line.strip()) if path.exists() else 0
            except OSError as exc:
                raise BrokerError(f"Не удалось прочитать партицию {partition}: {exc}") from exc
            self._sizes[partition] = cached
        return cached

    def total(self) -> int:
        return sum(self.size(partition) for partition in range(self.partitions))

    def _path(self, partition: int) -> Path:
        return self.root / f"partition-{partition}.jsonl"

    def _group_path(self, group: str) -> Path:
        safe = "".join(symbol for symbol in group if symbol.isalnum() or symbol in "-_") or "group"
        return self.root / "offsets" / f"{safe}.json"
