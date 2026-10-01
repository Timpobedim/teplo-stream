from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from teplo.meteo import Meteo, MeteoError, norm_supply_c

ANSWER = {"current": {"time": "2026-02-10T08:00", "interval": 900, "temperature_2m": -12.4}}


def client(handler: object) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))  # type: ignore[arg-type]


def test_temperature_is_read_and_cached(tmp_path: Path) -> None:
    cache = tmp_path / "погода.json"
    meteo = Meteo(cache=cache, client=client(lambda request: httpx.Response(200, json=ANSWER)))

    outside = meteo.current()
    assert outside.temperature_c == pytest.approx(-12.4)
    assert outside.source == "сеть"
    assert outside.measured_at == datetime(2026, 2, 10, 8, 0, tzinfo=UTC)
    assert json.loads(cache.read_text("utf-8"))["temperature_c"] == pytest.approx(-12.4)


def test_fresh_cache_is_used_without_network(tmp_path: Path) -> None:
    cache = tmp_path / "погода.json"
    cache.write_text(
        json.dumps(
            {
                "saved_at": datetime.now(UTC).isoformat(),
                "measured_at": datetime(2026, 2, 10, 8, 0, tzinfo=UTC).isoformat(),
                "temperature_c": -7.0,
            }
        ),
        "utf-8",
    )

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("свежий кеш не должен ходить в сеть")

    outside = Meteo(cache=cache, client=client(refuse)).current()
    assert (outside.temperature_c, outside.source) == (-7.0, "кеш")


def test_stale_cache_saves_the_run(tmp_path: Path) -> None:
    cache = tmp_path / "погода.json"
    cache.write_text(
        json.dumps(
            {
                "saved_at": (datetime.now(UTC) - timedelta(hours=3)).isoformat(),
                "measured_at": datetime(2026, 2, 10, 5, 0, tzinfo=UTC).isoformat(),
                "temperature_c": -3.0,
            }
        ),
        "utf-8",
    )

    def broken(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("сеть недоступна")

    outside = Meteo(cache=cache, client=client(broken)).current()
    assert (outside.temperature_c, outside.source) == (-3.0, "кеш")


def test_without_network_and_cache_it_says_so(tmp_path: Path) -> None:
    def broken(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("сеть недоступна")

    meteo = Meteo(cache=tmp_path / "нет.json", client=client(broken))
    with pytest.raises(MeteoError, match="Наружная температура недоступна"):
        meteo.current()


def test_server_error_is_not_mistaken_for_data(tmp_path: Path) -> None:
    meteo = Meteo(cache=tmp_path / "нет.json", client=client(lambda request: httpx.Response(503)))
    with pytest.raises(MeteoError):
        meteo.current()


@pytest.mark.parametrize(
    ("outside_c", "expected"),
    [(8.0, 60.0), (-28.0, 95.0), (-10.0, 77.5), (20.0, 60.0), (-40.0, 95.0)],
)
def test_supply_norm_follows_the_graph(outside_c: float, expected: float) -> None:
    assert norm_supply_c(outside_c) == pytest.approx(expected)
