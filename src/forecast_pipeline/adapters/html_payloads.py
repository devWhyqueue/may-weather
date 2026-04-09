"""HTML text extraction and hourly-to-daypart aggregation."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime

from bs4 import BeautifulSoup

from forecast_pipeline.models import DaypartForecast, ForecastDayparts

DAYPART_HOURS = {
    "morning": range(6, 12),
    "afternoon": range(12, 18),
    "evening": range(18, 24),
}
DAYPARTS = ("morning", "afternoon", "evening")
MONTHS_EN = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)
MONTHS_DE = (
    "Jan",
    "Feb",
    "Mär",
    "Apr",
    "Mai",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Okt",
    "Nov",
    "Dez",
)


@dataclass(frozen=True)
class PagePayload:
    """Raw HTTP or browser page content for a single source."""

    html: str
    text: str
    title: str | None
    final_url: str
    status_code: int | None


@dataclass(frozen=True)
class HourlyPoint:
    """One parsed hourly row before bucketing into dayparts."""

    local_time: datetime
    condition_summary: str | None
    precip_probability_pct: float | None
    sunshine_hours: float | None
    temperature_celsius: float | None = None


def _normalize_whitespace(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def _extract_title(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    if soup.title and soup.title.string:
        return _normalize_whitespace(soup.title.string)
    return None


def _strip_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(("script", "style", "noscript")):
        tag.decompose()
    return _normalize_whitespace(soup.get_text(" ", strip=True))


def _float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(",", ".").replace("&lt;", "").strip()
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    return float(match.group(0)) if match else None


def _extract_text_window(
    text: str, markers: tuple[str, ...], window: int = 320
) -> str | None:
    lowered = text.lower()
    for marker in markers:
        index = lowered.find(marker.lower())
        if index == -1:
            continue
        start = max(0, index - window)
        end = min(len(text), index + len(marker) + window)
        return text[start:end]
    return None


def _date_markers(target_date: date, language: str) -> tuple[str, ...]:
    month = (
        MONTHS_DE[target_date.month - 1]
        if language == "de"
        else MONTHS_EN[target_date.month - 1]
    )
    return (
        target_date.isoformat(),
        target_date.strftime("%d.%m."),
        target_date.strftime("%d.%m.%Y"),
        f"{target_date.day}. {month}",
        f"{month} {target_date.day}",
    )


def _extract_sunshine_hours(text: str) -> float | None:
    match = re.search(
        r"(\d+(?:[.,]\d+)?)\s*(?:h|Sonnenstunden|sunshine hours|hours of sunshine)",
        text,
        flags=re.IGNORECASE,
    )
    return _float(match.group(1)) if match else None


def _empty_dayparts() -> ForecastDayparts:
    return ForecastDayparts()


def _canonical_condition(value: str | None) -> str | None:
    if not value:
        return None
    lowered = value.lower()
    if any(
        token in lowered
        for token in ("rain", "regen", "drizzle", "sprinkles", "shower")
    ):
        return "Regen"
    if any(token in lowered for token in ("sun", "klar", "clear", "sonnig", "heiter")):
        return "Sonnig"
    if any(
        token in lowered for token in ("partly", "wechselnd", "wolkig", "teilweise")
    ):
        return "Wolkig"
    if any(token in lowered for token in ("cloud", "bewölkt", "bedeckt", "overcast")):
        return "Bewölkt"
    return value.strip().capitalize()


def _sunshine_from_condition(
    condition: str | None,
    *,
    uv_index: float | None = None,
    solar_elevation: float | None = None,
    cloud_cover: float | None = None,
) -> float:
    lowered = (condition or "").lower()
    if solar_elevation is not None and solar_elevation <= 0:
        return 0.0
    if any(
        token in lowered
        for token in ("rain", "regen", "drizzle", "sprinkles", "shower")
    ):
        return 0.0
    if any(token in lowered for token in ("sun", "klar", "clear", "sonnig", "heiter")):
        return 1.0
    if any(
        token in lowered for token in ("partly", "wechselnd", "wolkig", "teilweise")
    ):
        return 0.6
    if any(token in lowered for token in ("cloud", "bewölkt", "bedeckt", "overcast")):
        return 0.2 if (uv_index or 0) > 0 and (cloud_cover or 100) < 95 else 0.0
    if uv_index is not None and uv_index > 1:
        return 0.6
    return 0.0


def _aggregate_hourly(points: list[HourlyPoint], target_date: date) -> ForecastDayparts:
    grouped: dict[str, list[HourlyPoint]] = {daypart: [] for daypart in DAYPARTS}
    for point in points:
        if point.local_time.date() != target_date:
            continue
        for daypart, hours in DAYPART_HOURS.items():
            if point.local_time.hour in hours:
                grouped[daypart].append(point)
                break

    payload: dict[str, DaypartForecast] = {}
    for daypart in DAYPARTS:
        rows = grouped[daypart]
        if not rows:
            payload[daypart] = DaypartForecast()
            continue
        conditions = [
            row.condition_summary for row in rows if row.condition_summary is not None
        ]
        precip_values = [
            row.precip_probability_pct
            for row in rows
            if row.precip_probability_pct is not None
        ]
        sunshine_values = [
            row.sunshine_hours for row in rows if row.sunshine_hours is not None
        ]
        temp_values = [
            row.temperature_celsius
            for row in rows
            if row.temperature_celsius is not None
        ]
        payload[daypart] = DaypartForecast(
            condition_summary=Counter(conditions).most_common(1)[0][0]
            if conditions
            else None,
            precip_probability_pct=round(max(precip_values), 1)
            if precip_values
            else None,
            sunshine_hours=round(sum(sunshine_values), 1) if sunshine_values else None,
            temperature_celsius=round(sum(temp_values) / len(temp_values), 1)
            if temp_values
            else None,
        )
    return ForecastDayparts(**payload)
