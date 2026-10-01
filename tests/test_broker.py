from __future__ import annotations

from pathlib import Path

import pytest

from teplo.broker import Broker, BrokerError


def test_same_key_always_lands_in_one_partition(tmp_path: Path) -> None:
    first = Broker(tmp_path / "очередь", partitions=3)
    second = Broker(tmp_path / "очередь", partitions=3)
    assert first.partition_for("ИТП-Ленина-14:5") == second.partition_for("ИТП-Ленина-14:5")
    assert {first.partition_for(f"узел-{number}:1") for number in range(30)} == {0, 1, 2}


def test_published_events_are_read_back(tmp_path: Path) -> None:
    broker = Broker(tmp_path / "очередь", partitions=2)
    broker.publish_many([(f"узел:{number}", {"number": number}) for number in range(6)])
    assert broker.total() == 6

    records = broker.read("группа", limit=10)
    assert sorted(record.value["number"] for record in records) == [0, 1, 2, 3, 4, 5]
    assert all(record.offset >= 0 for record in records)


def test_partitions_share_the_batch(tmp_path: Path) -> None:
    broker = Broker(tmp_path / "очередь", partitions=3)
    for partition in range(3):
        keys = [key for key in (f"к{number}" for number in range(60)) if broker.partition_for(key) == partition]
        broker.publish_many([(key, {"key": key}) for key in keys[:10]])

    records = broker.read("группа", limit=3)
    assert len({record.partition for record in records}) == 3, "одна партиция не должна съедать всю пачку"


def test_commit_moves_the_group_forward(tmp_path: Path) -> None:
    broker = Broker(tmp_path / "очередь", partitions=2)
    broker.publish_many([(f"узел:{number}", {"number": number}) for number in range(4)])

    first = broker.read("группа", limit=10)
    broker.commit("группа", first)
    assert broker.read("группа", limit=10) == []
    assert sum(broker.lag("группа").values()) == 0

    broker.publish("узел:99", {"number": 99})
    again = broker.read("группа", limit=10)
    assert [record.value["number"] for record in again] == [99]


def test_offsets_survive_restart(tmp_path: Path) -> None:
    broker = Broker(tmp_path / "очередь", partitions=2)
    broker.publish_many([(f"узел:{number}", {"number": number}) for number in range(4)])
    broker.commit("группа", broker.read("группа", limit=10))

    restarted = Broker(tmp_path / "очередь", partitions=2)
    assert restarted.read("группа", limit=10) == []
    assert restarted.total() == 4


def test_other_group_reads_everything_again(tmp_path: Path) -> None:
    broker = Broker(tmp_path / "очередь", partitions=2)
    broker.publish_many([(f"узел:{number}", {"number": number}) for number in range(4)])
    broker.commit("первая", broker.read("первая", limit=10))
    assert len(broker.read("вторая", limit=10)) == 4


def test_broken_line_names_its_place(tmp_path: Path) -> None:
    broker = Broker(tmp_path / "очередь", partitions=1)
    broker.publish("узел:1", {"number": 1})
    path = tmp_path / "очередь" / "partition-0.jsonl"
    path.write_text(path.read_text("utf-8") + "{это не json}\n", "utf-8")

    with pytest.raises(BrokerError, match="Партиция 0, запись 1"):
        broker.read("группа", limit=10)


def test_mistakes_in_arguments_are_explained(tmp_path: Path) -> None:
    with pytest.raises(BrokerError, match="хотя бы одна"):
        Broker(tmp_path / "очередь", partitions=0)
    broker = Broker(tmp_path / "очередь")
    with pytest.raises(BrokerError, match="положительное число"):
        broker.read("группа", limit=0)
