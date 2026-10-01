from __future__ import annotations

from pathlib import Path

import pytest

from teplo import demo
from teplo.cli import main


@pytest.fixture
def stand(tmp_path: Path) -> Path:
    root = tmp_path / "стенд"
    assert main(["produce", "--root", str(root), "--minutes", "30", "--offline"]) == 0
    assert main(["run", "--root", str(root)]) == 0
    return root


def test_produce_fills_the_queue(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = tmp_path / "стенд"
    assert main(["produce", "--root", str(root), "--minutes", "10", "--offline"]) == 0
    assert (root / "очередь").exists()
    assert "В очередь положено" in capsys.readouterr().out


def test_run_reports_every_batch(stand: Path, capsys: pytest.CaptureFixture[str]) -> None:
    capsys.readouterr()
    assert main(["run", "--root", str(stand)]) == 0
    assert "нечего обрабатывать" in capsys.readouterr().out, "очередь уже прочитана целиком"


def test_report_prints_nodes_and_writes_a_page(stand: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    page = tmp_path / "дашборд.html"
    assert main(["report", "--root", str(stand), "--offline", "--html", str(page)]) == 0

    printed = capsys.readouterr().out
    assert "ИТП-Садовая-3" in printed
    assert "Витрина:" in printed
    html = page.read_text("utf-8")
    assert "Теплосеть" in html
    assert "<svg" in html


def test_alerts_exit_code_marks_an_alarm(stand: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["alerts", "--root", str(stand), "--offline"])
    printed = capsys.readouterr().out
    assert code == 1, "при аварии код возврата должен быть ненулевым — это удобно в планировщике"
    assert "ИТП-Садовая-3" in printed


def test_empty_stand_is_explained(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    empty = tmp_path / "пусто"
    assert main(["run", "--root", str(empty)]) == 0
    assert "нечего обрабатывать" in capsys.readouterr().out
    assert main(["report", "--root", str(empty), "--offline"]) == 0
    assert "Витрина пуста" in capsys.readouterr().out


def test_demo_runs_end_to_end(tmp_path: Path) -> None:
    result = demo.run(root=tmp_path / "демо", minutes=20.0, offline=True)
    assert result.dashboard.exists()
    assert "Витрина:" in result.text
    assert sum(batch.stored for batch in result.batches) > 100
    assert sum(batch.duplicates for batch in result.batches) > 0, "в демо есть повторы — их должно быть видно"
