from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ForecastMetrics:
    """Normalized weather metrics shared by every source and the blended forecast."""

    temp_min_c: float | None = None
    temp_max_c: float | None = None
    precip_probability_pct: float | None = None
    precip_mm: float | None = None
    wind_kph: float | None = None
    condition_summary: str | None = None


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
    metrics: ForecastMetrics = field(default_factory=ForecastMetrics)

    def to_dict(self) -> dict[str, Any]:
        """Flatten nested metrics into the JSON shape consumed by the frontend."""

        payload = asdict(self)
        payload.update(payload.pop("metrics"))
        return payload


@dataclass(frozen=True)
class ConsensusForecast:
    """Weighted summary of the currently usable source forecasts."""

    status: str
    label: str
    note: str
    source_count: int
    confidence: float
    spread: dict[str, float | None]
    metrics: ForecastMetrics = field(default_factory=ForecastMetrics)

    def to_dict(self) -> dict[str, Any]:
        """Flatten nested metrics while explicitly preserving the source-count field."""

        payload = asdict(self)
        payload.update(payload.pop("metrics"))
        payload["label"] = self.label
        payload["source_count"] = self.source_count
        return payload


def now_utc_iso() -> str:
    """Return the current UTC time in ISO-8601 format with a trailing Z suffix."""

    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
