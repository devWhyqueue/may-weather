from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SITE_DIR = PROJECT_ROOT / "site"
DATA_DIR = SITE_DIR / "data"
TARGET_DATE = date(2026, 5, 1)


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
