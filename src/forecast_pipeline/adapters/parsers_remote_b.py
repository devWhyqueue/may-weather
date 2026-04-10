"""Site-specific parsers: grid tables, Das Wetter, Open-Meteo, and parser registry."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import date, datetime

from bs4 import BeautifulSoup

from forecast_pipeline.config import LOCATION
from forecast_pipeline.models import ForecastDayparts

from .html_payloads import (
    HourlyPoint,
    PagePayload,
    _aggregate_hourly,
    _canonical_condition,
    _float,
    _sunshine_from_condition,
)
from .html_regions import _parse_wetteronline
from .parsers_remote_a import (
    _parse_foreca,
    _parse_msn,
    _parse_timeanddate,
    _parse_weatherandradar,
    _parse_wetterde,
    _parse_wettertv,
    _parse_ventusky,
)
from .parsers_remote_com import _parse_weathercom, _parse_yr


def _parse_meteoblue(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    soup = BeautifulSoup(page.html, "html.parser")
    points: list[HourlyPoint] = []
    for table in soup.select("table.picto.hourly-view"):
        times = [
            time_tag.get("datetime")
            for time_tag in table.select("tr.times time")
            if time_tag.get("datetime")
        ]
        icons = [img.get("alt") for img in table.select("tr.icons img")]
        probs = [
            _float(cell.get_text(" ", strip=True))
            for cell in table.select("tr.precipitationprobabilities td")
        ]
        temp_row = table.select_one("tr.temperature") or table.select_one(
            "tr[class*='temperature']"
        )
        temps: list[float | None] = []
        if temp_row:
            temps = [
                _float(td.get_text(" ", strip=True)) for td in temp_row.select("td")
            ]
        for idx, (valid_time, icon, probability) in enumerate(
            zip(times, icons, probs, strict=False)
        ):
            local_time = datetime.fromisoformat(str(valid_time))
            air = temps[idx] if idx < len(temps) else None
            points.append(
                HourlyPoint(
                    local_time=local_time.replace(tzinfo=None),
                    condition_summary=_canonical_condition(
                        str(icon) if icon is not None else None
                    ),
                    precip_probability_pct=probability,
                    sunshine_hours=_sunshine_from_condition(
                        str(icon) if icon is not None else None
                    ),
                    temperature_celsius=air,
                )
            )
    return _aggregate_hourly(points, target_date) if points else None


def _parse_daswetter(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    points: list[HourlyPoint] = []
    target_marker = target_date.strftime("%d.%m.%Y")
    text = page.text
    if target_marker not in text and target_date.strftime("%d.%m.") not in text:
        text = page.text
    pattern = re.compile(
        r"(?P<hour>\d{2}:\d{2})\s+(?:(?P<prob>\d+)%\s+(?:[\d.,]+\s*mm)\s+)?(?P<temp>\d+)°\s+(?P<cond>[A-Za-zäöüÄÖÜß ]+?)\s+gefühlte",
        re.IGNORECASE,
    )
    for match in pattern.finditer(text):
        hour, minute = [int(part) for part in match.group("hour").split(":")]
        if hour >= 24:
            continue
        condition = _canonical_condition(match.group("cond"))
        points.append(
            HourlyPoint(
                local_time=datetime(
                    target_date.year, target_date.month, target_date.day, hour, minute
                ),
                condition_summary=condition,
                precip_probability_pct=_float(match.group("prob")) or 0.0,
                sunshine_hours=_sunshine_from_condition(match.group("cond")),
                temperature_celsius=_float(match.group("temp")),
            )
        )
    return _aggregate_hourly(points, target_date) if points else None


def _openmeteo_condition_from_code(code: int) -> str:
    if code == 0:
        return "Sonnig"
    if code in (1, 2, 3):
        return "Wolkig"
    if code in (45, 48):
        return "Bewölkt"
    if 51 <= code <= 67 or 80 <= code <= 86 or code in (95, 96, 99):
        return "Regen"
    if 71 <= code <= 77:
        return "Regen"
    return "Bewölkt"


def _parse_openmeteo(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    try:
        data = json.loads(page.text)
    except json.JSONDecodeError:
        return None
    hourly = data.get("hourly")
    if not isinstance(hourly, dict):
        return None
    times = hourly.get("time")
    temps = hourly.get("temperature_2m")
    pps = hourly.get("precipitation_probability")
    codes = hourly.get("weather_code")
    if not isinstance(times, list) or not isinstance(temps, list):
        return None
    points: list[HourlyPoint] = []
    for i, tstr in enumerate(times):
        if i >= len(temps) or temps[i] is None:
            continue
        pp_raw = pps[i] if isinstance(pps, list) and i < len(pps) else 0
        code_raw = codes[i] if isinstance(codes, list) and i < len(codes) else 0
        try:
            code_int = int(code_raw)
        except (TypeError, ValueError):
            code_int = 0
        cond = _openmeteo_condition_from_code(code_int)
        local_time = datetime.fromisoformat(str(tstr))
        if local_time.tzinfo is not None:
            local_time = local_time.replace(tzinfo=None)
        points.append(
            HourlyPoint(
                local_time=local_time,
                condition_summary=cond,
                precip_probability_pct=float(pp_raw) if pp_raw is not None else 0.0,
                sunshine_hours=_sunshine_from_condition(cond),
                temperature_celsius=_float(temps[i]),
            )
        )
    return _aggregate_hourly(points, target_date) if points else None


def openmeteo_forecast_url() -> str:
    """Build the Open-Meteo API URL for the configured location."""

    loc = LOCATION
    tz = loc.timezone.replace("/", "%2F")
    return (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={loc.latitude}&longitude={loc.longitude}"
        "&hourly=temperature_2m,precipitation_probability,weather_code"
        f"&timezone={tz}&forecast_days=16"
    )


SOURCE_PARSERS: dict[str, Callable[[PagePayload, date], ForecastDayparts | None]] = {
    "wetteronline": _parse_wetteronline,
    "meteoblue": _parse_meteoblue,
    "daswetter": _parse_daswetter,
    "timeanddate": _parse_timeanddate,
    "weathercom": _parse_weathercom,
    "yr": _parse_yr,
    "foreca": _parse_foreca,
    "msn": _parse_msn,
    "weatherandradar": _parse_weatherandradar,
    "wetterde": _parse_wetterde,
    "ventusky": _parse_ventusky,
    "wettertv": _parse_wettertv,
    "openmeteo": _parse_openmeteo,
}
