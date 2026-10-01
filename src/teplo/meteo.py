"""Наружная температура: без неё нельзя сказать, нормальна подача или котельная недотапливает.

Открытый источник Open-Meteo, без ключей и регистрации. Сеть может не ответить — тогда берётся
последнее сохранённое значение, а если и его нет, правила, зависящие от улицы, просто отключаются:
остальной конвейер от этого не останавливается.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

URL = "https://api.open-meteo.com/v1/forecast"
TIMEOUT = httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0)
TTL_MINUTES = 30.0
MOSCOW = (55.7558, 37.6173)


class MeteoError(RuntimeError):
    """Наружную температуру получить не удалось — ни из сети, ни из кеша."""


@dataclass(frozen=True, slots=True)
class Outside:
    measured_at: datetime
    temperature_c: float
    source: str


def norm_supply_c(outside_c: float) -> float:
    """Упрощённый температурный график 95/70: при +8 °C подача 60 °C, при −28 °C — 95 °C.

    Настоящий график у каждой теплосети свой и задаётся таблицей; линейная прикидка нужна, чтобы
    правило «недотоп» вообще имело точку отсчёта, и в отчёте она названа прикидкой.
    """
    share = (8.0 - outside_c) / 36.0
    return round(min(max(60.0 + share * 35.0, 60.0), 95.0), 1)


class Meteo:
    """Клиент Open-Meteo с кешем на диске и понятным поведением при недоступности сети."""

    def __init__(
        self,
        latitude: float = MOSCOW[0],
        longitude: float = MOSCOW[1],
        *,
        cache: Path | None = None,
        ttl_minutes: float = TTL_MINUTES,
        client: httpx.Client | None = None,
    ) -> None:
        self.latitude = latitude
        self.longitude = longitude
        self.cache = cache
        self.ttl = timedelta(minutes=ttl_minutes)
        self._client = client

    def current(self) -> Outside:
        """Свежее значение из кеша, иначе из сети, иначе — устаревшее из кеша."""
        fresh = self._cached(fresh_only=True)
        if fresh is not None:
            return fresh
        try:
            outside = self._fetch()
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            stale = self._cached(fresh_only=False)
            if stale is not None:
                return stale
            raise MeteoError(f"Наружная температура недоступна: {exc}") from exc
        self._store(outside)
        return outside

    def _fetch(self) -> Outside:
        params = {
            "latitude": f"{self.latitude}",
            "longitude": f"{self.longitude}",
            "current": "temperature_2m",
            "timezone": "UTC",
        }
        client = self._client or httpx.Client(timeout=TIMEOUT, headers={"User-Agent": "teplo-stream/1.0"})
        try:
            response = client.get(URL, params=params)
            response.raise_for_status()
            current = response.json()["current"]
            moment = datetime.fromisoformat(str(current["time"])).replace(tzinfo=UTC)
            return Outside(measured_at=moment, temperature_c=float(current["temperature_2m"]), source="сеть")
        finally:
            if self._client is None:
                client.close()

    def _cached(self, *, fresh_only: bool) -> Outside | None:
        if self.cache is None or not self.cache.exists():
            return None
        try:
            raw = json.loads(self.cache.read_text("utf-8"))
            saved_at = datetime.fromisoformat(str(raw["saved_at"]))
            outside = Outside(
                measured_at=datetime.fromisoformat(str(raw["measured_at"])),
                temperature_c=float(raw["temperature_c"]),
                source="кеш",
            )
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            return None
        if fresh_only and datetime.now(UTC) - saved_at > self.ttl:
            return None
        return outside

    def _store(self, outside: Outside) -> None:
        if self.cache is None:
            return
        payload = {
            "saved_at": datetime.now(UTC).isoformat(),
            "measured_at": outside.measured_at.isoformat(),
            "temperature_c": outside.temperature_c,
        }
        tmp = self.cache.with_suffix(".tmp")
        try:
            self.cache.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")
            os.replace(tmp, self.cache)
        except OSError:
            return
