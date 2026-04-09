from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class DaypartForecast:
    """Normalized forecast values for one part of the day."""

    condition_summary: str | None = None
    precip_probability_pct: float | None = None
    sunshine_hours: float | None = None
    temperature_celsius: float | None = None


@dataclass(frozen=True)
class ForecastDayparts:
    """Normalized weather values split into morning, afternoon, and evening."""

    morning: DaypartForecast = field(default_factory=DaypartForecast)  # noqa
    afternoon: DaypartForecast = field(default_factory=DaypartForecast)  # noqa
    evening: DaypartForecast = field(default_factory=DaypartForecast)  # noqa


@dataclass(frozen=True)
class SourceForecast:
    """One normalized source record written into the static JSON output."""

    source_id: str
    source_name: str
    fetched_at: str
    target_date: str
    source_url: str
    method: str
    confidence: float
    status: str
    note: str | None = None
    dayparts: ForecastDayparts = field(default_factory=ForecastDayparts)
    ranking_eligible: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize this record to a JSON-friendly plain dict."""

        return asdict(self)


@dataclass(frozen=True)
class ConsensusForecast:
    """Single-provider or legacy summary written as best_forecast in static JSON."""

    status: str
    label: str  # noqa
    note: str
    source_count: int  # noqa
    confidence: float
    spread: dict[str, dict[str, float | None]]  # noqa
    dayparts: ForecastDayparts = field(default_factory=ForecastDayparts)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this record to a JSON-friendly plain dict."""

        return asdict(self)


@dataclass(frozen=True)
class SelectedDisplaySource:
    """The provider currently shown on the microsite (optimistic pick)."""

    source_id: str
    source_name: str
    source_url: str


def now_utc_iso() -> str:
    """Return the current UTC time in ISO-8601 format with a trailing Z suffix."""

    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
