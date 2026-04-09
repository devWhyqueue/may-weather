from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path


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


@dataclass(frozen=True)
class SourceDefinition:
    """Describes one external weather source and how strongly it should be weighted."""

    source_id: str
    source_name: str
    source_url: str
    method: str
    weight: float
    horizon_days: int | None = None
    fetch_mode: str = "http"
    language: str = "de"
    location_markers: tuple[str, ...] = ()
    invalid_markers: tuple[str, ...] = ()
    wait_for_ms: int = 0


COMMON_INVALID_MARKERS = (
    "access denied",
    "just a moment",
    "error 404",
    "http status 404",
    "die website ist nicht erreichbar",
    "search results",
    "suchergebnisse",
)


SOURCE_DEFINITIONS = (
    SourceDefinition(
        source_id="wetteronline",
        source_name="WetterOnline",
        source_url="https://www.wetteronline.de/wetter/haltern-am-see",
        method="text",
        weight=0.95,
        horizon_days=14,
        language="de",
        location_markers=("Haltern am See",),
        invalid_markers=COMMON_INVALID_MARKERS,
    ),
    SourceDefinition(
        source_id="meteoblue",
        source_name="meteoblue",
        source_url="https://www.meteoblue.com/en/weather/today/haltern-am-see_germany_2911396",
        method="html",
        weight=0.9,
        horizon_days=14,
        language="en",
        location_markers=("Haltern am See",),
        invalid_markers=COMMON_INVALID_MARKERS,
    ),
    SourceDefinition(
        source_id="daswetter",
        source_name="Das Wetter / Meteored",
        source_url="https://www.daswetter.com/wetter_Haltern%2Bam%2BSee-Europa-Deutschland-Nordrhein%2BWestfalen--1-26703.html",
        method="text",
        weight=0.87,
        horizon_days=14,
        language="de",
        location_markers=("Haltern am See",),
        invalid_markers=COMMON_INVALID_MARKERS,
    ),
    SourceDefinition(
        source_id="timeanddate",
        source_name="timeanddate",
        source_url="https://www.timeanddate.com/weather/germany/haltern-am-see/hourly",
        method="html",
        weight=0.84,
        horizon_days=14,
        language="en",
        location_markers=("Haltern am See",),
        invalid_markers=COMMON_INVALID_MARKERS,
    ),
    SourceDefinition(
        source_id="weathercom",
        source_name="weather.com",
        source_url="https://weather.com/de-DE/wetter/stundlich/l/4f2a336086cc19d90f233d01483a462f5d7537133fe126349db13fb3727b75d6",
        method="html",
        weight=0.86,
        horizon_days=10,
        language="de",
        location_markers=("Haltern am See",),
        invalid_markers=COMMON_INVALID_MARKERS,
    ),
    SourceDefinition(
        source_id="yr",
        source_name="yr.no",
        source_url="https://www.yr.no/en/forecast/hourly-table/2-2911396/Germany/North%20Rhine-Westphalia/Regierungsbezirk%20M%C3%BCnster/Haltern%20am%20See",
        method="html",
        weight=0.79,
        horizon_days=10,
        language="en",
        location_markers=("Haltern am See",),
        invalid_markers=COMMON_INVALID_MARKERS,
    ),
    SourceDefinition(
        source_id="openmeteo",
        source_name="Open-Meteo",
        source_url="https://open-meteo.com",
        method="json",
        weight=0.92,
        horizon_days=16,
        language="en",
        location_markers=(),
        invalid_markers=(),
        fetch_mode="openmeteo",
    ),
    SourceDefinition(
        source_id="weatherandradar",
        source_name="Weather & Radar",
        source_url="https://www.weatherandradar.com/weather/haltern-am-see/5497243",
        method="html",
        weight=0.75,
        horizon_days=10,
        language="en",
        location_markers=("Haltern am See",),
        invalid_markers=COMMON_INVALID_MARKERS,
    ),
    SourceDefinition(
        source_id="ventusky",
        source_name="Ventusky",
        source_url="https://www.ventusky.com/haltern-am-see",
        method="html",
        weight=0.74,
        horizon_days=10,
        language="de",
        location_markers=("Haltern am See",),
        invalid_markers=COMMON_INVALID_MARKERS,
    ),
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
