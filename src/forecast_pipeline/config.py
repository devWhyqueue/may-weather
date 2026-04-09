from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from forecast_pipeline.adapters.catalog import SOURCE_DEFINITIONS


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SITE_DIR = PROJECT_ROOT / "site"
DATA_DIR = SITE_DIR / "data"


@dataclass(frozen=True)
class Location:
    name: str
    country: str
    region: str
    latitude: float
    longitude: float
    timezone: str


LOCATION = Location(
    name="Haltern am See",
    country="Germany",
    region="North Rhine-Westphalia",
    latitude=51.7435,
    longitude=7.1815,
    timezone="Europe/Berlin",
)


def max_horizon_days() -> int:
    """Return the furthest configured forecast horizon across all configured sources."""

    horizons = [
        definition.horizon_days
        for definition in SOURCE_DEFINITIONS
        if definition.horizon_days is not None
    ]
    return max(horizons, default=14)


def preferred_target_date(today: date | None = None) -> date:
    """Return the May 1 date the site should try to approximate."""

    today = today or date.today()
    candidate = date(today.year, 5, 1)
    if candidate < today - timedelta(days=max_horizon_days()):
        return date(today.year + 1, 5, 1)
    return candidate
