"""Site-specific parsers: hourly tables and segment summaries (batch A)."""

from __future__ import annotations

import re
from datetime import date, datetime

from bs4 import BeautifulSoup

from forecast_pipeline.models import DaypartForecast, ForecastDayparts

from .html_payloads import (
    HourlyPoint,
    PagePayload,
    _aggregate_hourly,
    _canonical_condition,
    _float,
    _normalize_whitespace,
    _sunshine_from_condition,
)
from .html_regions import _json_longest_segment

# Weather & Radar: day-interval objects embedded in the SSR HTML (morning/afternoon/evening).
_WAR_INTERVAL_RE = re.compile(
    r'\{"air_pressure":\{[^}]+\}.{0,400}?'
    r'"air_temperature":\{"celsius":(?P<temp>-?\d+)[^}]*\}.{0,400}?'
    r'"date":"(?P<ds>\d{4}-\d{2}-\d{2})T[^"]+".{0,500}?'
    r'"precipitation":\{"probability":(?P<prob>[\d.]+)[^}]*\}.{0,400}?'
    r'"symbol":"(?P<sym>[^"]+)".{0,200}?'
    r'"type":"(?P<typ>morning|afternoon|evening)"',
    re.DOTALL,
)


def _parse_timeanddate(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    points: list[HourlyPoint] = []
    day_marker = target_date.strftime("%d. %b")
    pattern = re.compile(
        r"(?P<hour>\d{2}:\d{2})\s+(?P<temp>\d+)\s*°C\s+(?P<desc>[A-Za-z. ]+)\s+\d+\s*°C\s+\d+\s*km/h\s+\S+\s+\d+%\s+(?P<pc>\d+)%",
        re.IGNORECASE,
    )
    if day_marker not in page.text:
        return None
    for match in pattern.finditer(page.text):
        hour, minute = [int(part) for part in match.group("hour").split(":")]
        condition = _canonical_condition(match.group("desc"))
        points.append(
            HourlyPoint(
                local_time=datetime(
                    target_date.year, target_date.month, target_date.day, hour, minute
                ),
                condition_summary=condition,
                precip_probability_pct=float(match.group("pc")),
                sunshine_hours=_sunshine_from_condition(match.group("desc")),
                temperature_celsius=_float(match.group("temp")),
            )
        )
    return _aggregate_hourly(points, target_date) if points else None


def _parse_foreca(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    points: list[HourlyPoint] = []
    pattern = re.compile(
        r'\{"time":"(?P<time>[^"]+)".*?"rainp":(?P<rainp>[\d.]+).*?"uvi":(?P<uvi>[\d.]+).*?"cloud":"(?P<cloud>[^"]+)".*?"wx":"(?P<wx>[^"]+)"',
    )
    for match in pattern.finditer(page.html):
        local_time = datetime.fromisoformat(match.group("time"))
        condition = _canonical_condition(match.group("wx"))
        points.append(
            HourlyPoint(
                local_time=local_time,
                condition_summary=condition,
                precip_probability_pct=_float(match.group("rainp")),
                sunshine_hours=_sunshine_from_condition(
                    match.group("wx"),
                    uv_index=_float(match.group("uvi")),
                    cloud_cover=_float(match.group("cloud")),
                ),
            )
        )
    return _aggregate_hourly(points, target_date) if points else None


def _parse_msn(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    payload = _json_longest_segment(page.html, '"hourly":')
    if not isinstance(payload, list):
        return None
    points: list[HourlyPoint] = []
    for item in payload:
        if not isinstance(item, dict) or "timeStr" not in item:
            continue
        local_time = datetime.fromisoformat(str(item["timeStr"]))
        summary = item.get("cap") or item.get("summary")
        condition = _canonical_condition(str(summary))
        points.append(
            HourlyPoint(
                local_time=local_time,
                condition_summary=condition,
                precip_probability_pct=_float(item.get("precipitation")) or 0.0,
                sunshine_hours=_sunshine_from_condition(
                    str(summary),
                    uv_index=_float(item.get("uv")),
                    cloud_cover=_float(item.get("cloudCover")),
                ),
            )
        )
    return _aggregate_hourly(points, target_date) if points else None


def _parse_weatherandradar(
    page: PagePayload, target_date: date
) -> ForecastDayparts | None:
    wanted = target_date.isoformat()
    by_part: dict[str, tuple[float, float, str]] = {}
    # Intervals are embedded in raw HTML/JSON; stripped `page.text` often omits them.
    for match in _WAR_INTERVAL_RE.finditer(page.html):
        if match.group("ds") != wanted:
            continue
        typ = match.group("typ")
        temp = float(match.group("temp"))
        prob = float(match.group("prob")) * 100.0
        sym = match.group("sym")
        cond = _canonical_condition(sym) or "Bewölkt"
        by_part[typ] = (temp, prob, cond)

    if {"morning", "afternoon", "evening"}.issubset(by_part):

        def _dp(key: str) -> DaypartForecast:
            t_c, p_c, c = by_part[key]
            sun = (
                _sunshine_from_condition(c)
                * 3.0
                * (0.3 if key == "morning" else (0.45 if key == "afternoon" else 0.25))
            )
            return DaypartForecast(
                condition_summary=c,
                precip_probability_pct=round(p_c, 1),
                sunshine_hours=round(sun, 1),
                temperature_celsius=round(t_c, 1),
            )

        return ForecastDayparts(
            morning=_dp("morning"),
            afternoon=_dp("afternoon"),
            evening=_dp("evening"),
        )

    summary_match = re.search(
        r"Morning\s+(?P<tm>\d+)\s*°\s+(?P<morning>\d+)\s*%\s+Afternoon\s+(?P<ta>\d+)\s*°\s+(?P<afternoon>\d+)\s*%\s+Evening\s+(?P<te>\d+)\s*°\s+(?P<evening>\d+)\s*%",
        page.html,
    )
    sunshine_match = re.search(
        r'aria-label="(?P<hours>\d+)hours of sunshine"', page.html
    )
    if not summary_match:
        return None
    sunshine_total = _float(sunshine_match.group("hours")) if sunshine_match else 3.0
    sunshine_total = sunshine_total or 3.0
    return ForecastDayparts(
        morning=DaypartForecast(
            condition_summary="Bewölkt",
            precip_probability_pct=_float(summary_match.group("morning")),
            sunshine_hours=round(sunshine_total * 0.3, 1),
            temperature_celsius=_float(summary_match.group("tm")),
        ),
        afternoon=DaypartForecast(
            condition_summary="Wolkig",
            precip_probability_pct=_float(summary_match.group("afternoon")),
            sunshine_hours=round(sunshine_total * 0.45, 1),
            temperature_celsius=_float(summary_match.group("ta")),
        ),
        evening=DaypartForecast(
            condition_summary="Bewölkt",
            precip_probability_pct=_float(summary_match.group("evening")),
            sunshine_hours=round(sunshine_total * 0.25, 1),
            temperature_celsius=_float(summary_match.group("te")),
        ),
    )


def _parse_ventusky(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    soup = BeautifulSoup(page.html, "html.parser")
    forecast = soup.find("div", id="forecast_24")
    if forecast is None:
        return None
    headers = forecast.select("thead th")
    cells = forecast.select("tbody tr:first-child td")
    points: list[HourlyPoint] = []
    today = date.today()
    delta = (target_date - today).days
    if delta < 0 or delta > 1:
        return None
    header_cells = headers if len(headers) == len(cells) else headers[1:]
    for header, cell in zip(header_cells, cells, strict=False):
        header_text = _normalize_whitespace(header.get_text(" ", strip=True))
        hlow = header_text.lower()
        is_next = "morgen" in hlow or "tomorrow" in hlow
        if delta == 0 and is_next:
            continue
        if delta == 1 and not is_next:
            continue
        time_match = re.search(r"(\d{2}):(\d{2})", header_text)
        if not time_match:
            continue
        hour = int(time_match.group(1))
        minute = int(time_match.group(2))
        img = cell.find("img")
        condition_raw = (
            str(img.get("alt")) if img and img.get("alt") is not None else None
        )
        prob = None
        prob_node = cell.find("span", class_="prob-line")
        if prob_node:
            prob = _float(prob_node.get_text(" ", strip=True))
        cell_text = cell.get_text(" ", strip=True)
        temp_m = re.search(r"(-?\d+(?:[.,]\d+)?)\s*°", cell_text)
        air_temp = _float(temp_m.group(1)) if temp_m else None
        points.append(
            HourlyPoint(
                local_time=datetime(
                    target_date.year, target_date.month, target_date.day, hour, minute
                ),
                condition_summary=_canonical_condition(condition_raw),
                precip_probability_pct=prob,
                sunshine_hours=_sunshine_from_condition(condition_raw),
                temperature_celsius=air_temp,
            )
        )
    return _aggregate_hourly(points, target_date) if points else None
