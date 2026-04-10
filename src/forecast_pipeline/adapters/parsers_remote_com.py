"""weather.com and yr.no parsers (HTML hourly + embedded JSON / API JSON)."""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta

from forecast_pipeline.models import DaypartForecast, ForecastDayparts

from .html_payloads import (
    HourlyPoint,
    PagePayload,
    _aggregate_hourly,
    _balanced_json_array,
    _canonical_condition,
    _float,
    _sunshine_from_condition,
)


def _parse_weathercom_tenday(
    page: PagePayload, target_date: date
) -> ForecastDayparts | None:
    raw = page.html
    m_max = re.search(r'\\"calendarDayTemperatureMax\\":\s*\[([^\]]+)\]', raw)
    m_min = re.search(r'\\"calendarDayTemperatureMin\\":\s*\[([^\]]+)\]', raw)
    if not m_max or not m_min:
        return None

    def _ints(blob: str) -> list[int]:
        return [
            int(x.strip()) for x in blob.split(",") if x.strip().lstrip("-").isdigit()
        ]

    highs = _ints(m_max.group(1))
    lows = _ints(m_min.group(1))
    if not highs or len(lows) < len(highs):
        return None
    m_moon = re.search(r'\\"moonriseTimeLocal\\":\s*\[', raw)
    if not m_moon:
        return None
    bracket_start = raw.find("[", m_moon.start())
    moon_slice = _balanced_json_array(raw, bracket_start)
    if moon_slice is None:
        return None
    day_strings = re.findall(r'\\"(\d{4}-\d{2}-\d{2})T', moon_slice)
    if len(day_strings) != len(highs):
        return None
    want = target_date.isoformat()
    if want not in day_strings:
        return None
    idx = day_strings.index(want)
    hi = float(highs[idx])
    lo = float(lows[idx]) if idx < len(lows) else hi
    rain_pct = 30.0
    m_pc = re.search(r'\\"precipChance\\":\s*\[([^\]]+)\]', raw)
    if m_pc:
        cells = [c.strip() for c in m_pc.group(1).split(",")]
        if idx < len(cells) and cells[idx] not in ("", "null") and cells[idx].isdigit():
            rain_pct = float(cells[idx])
    condition = (
        "Regen" if rain_pct >= 55 else ("Wolkig" if rain_pct >= 35 else "Bewölkt")
    )
    sun_total = _sunshine_from_condition(condition) * 3.0
    t_morning = lo + (hi - lo) * 0.35
    t_afternoon = hi
    t_evening = lo + (hi - lo) * 0.22
    return ForecastDayparts(
        morning=DaypartForecast(
            condition_summary=condition,
            precip_probability_pct=rain_pct,
            sunshine_hours=round(sun_total * 0.3, 1),
            temperature_celsius=round(t_morning, 1),
        ),
        afternoon=DaypartForecast(
            condition_summary=condition,
            precip_probability_pct=rain_pct,
            sunshine_hours=round(sun_total * 0.45, 1),
            temperature_celsius=round(t_afternoon, 1),
        ),
        evening=DaypartForecast(
            condition_summary=condition,
            precip_probability_pct=rain_pct,
            sunshine_hours=round(sun_total * 0.25, 1),
            temperature_celsius=round(t_evening, 1),
        ),
    )


def _parse_weathercom(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    if "calendarDayTemperatureMax" in page.html:
        tenday = _parse_weathercom_tenday(page, target_date)
        if tenday is not None:
            return tenday
        return None
    points: list[HourlyPoint] = []
    current_date = date.today()
    previous_hour = -1
    pattern = re.compile(
        r"(?P<hour>\d{2}:\d{2})\s+(?P<cond>[A-Za-zäöüÄÖÜß ]+)\s+(?P<air>\d+)\s*°\s+Rain drop\s+(?P<pc>\d+)%.*?UV-Index\s+(?P<uv>\d+)",
        re.IGNORECASE,
    )
    for match in pattern.finditer(page.html):
        hour, minute = [int(part) for part in match.group("hour").split(":")]
        if previous_hour > hour:
            current_date = current_date + timedelta(days=1)
        previous_hour = hour
        condition = _canonical_condition(match.group("cond"))
        points.append(
            HourlyPoint(
                local_time=datetime(
                    current_date.year,
                    current_date.month,
                    current_date.day,
                    hour,
                    minute,
                ),
                condition_summary=condition,
                precip_probability_pct=_float(match.group("pc")),
                sunshine_hours=_sunshine_from_condition(
                    match.group("cond"),
                    uv_index=_float(match.group("uv")),
                ),
                temperature_celsius=_float(match.group("air")),
            )
        )
    return _aggregate_hourly(points, target_date) if points else None


def _parse_yr(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    try:
        data = json.loads(page.text)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    points: list[HourlyPoint] = []

    def _naive_local(ts: str) -> datetime:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt

    for row in data.get("shortIntervals", []):
        if not isinstance(row, dict):
            continue
        start_s = row.get("start")
        if not start_s:
            continue
        local_time = _naive_local(str(start_s))
        if local_time.date() != target_date:
            continue
        sym = row.get("symbolCode", {})
        next1 = str(sym.get("next1Hour") or "") if isinstance(sym, dict) else ""
        precip = row.get("precipitation", {})
        precip_val = _float(precip.get("value")) if isinstance(precip, dict) else 0.0
        uv = row.get("uvIndex", {})
        uv_val = _float(uv.get("value")) if isinstance(uv, dict) else None
        cloud = row.get("cloudCover", {})
        cloud_val = _float(cloud.get("value")) if isinstance(cloud, dict) else None
        temp = row.get("temperature", {})
        air = _float(temp.get("value")) if isinstance(temp, dict) else None
        points.append(
            HourlyPoint(
                local_time=local_time,
                condition_summary=_canonical_condition(next1),
                precip_probability_pct=100.0 if (precip_val or 0) > 0 else 0.0,
                sunshine_hours=_sunshine_from_condition(
                    next1,
                    uv_index=uv_val,
                    cloud_cover=cloud_val,
                ),
                temperature_celsius=air,
            )
        )
    for row in data.get("longIntervals", []):
        if not isinstance(row, dict):
            continue
        start_s = row.get("start")
        if not start_s:
            continue
        local_time = _naive_local(str(start_s))
        if local_time.date() != target_date:
            continue
        sym = row.get("symbolCode", {})
        next1 = ""
        if isinstance(sym, dict):
            next1 = str(sym.get("next1Hour") or sym.get("next6Hours") or "")
        precip = row.get("precipitation", {})
        precip_val = _float(precip.get("value")) if isinstance(precip, dict) else 0.0
        temp = row.get("temperature", {})
        air = _float(temp.get("value")) if isinstance(temp, dict) else None
        points.append(
            HourlyPoint(
                local_time=local_time,
                condition_summary=_canonical_condition(next1),
                precip_probability_pct=100.0 if (precip_val or 0) > 0 else 0.0,
                sunshine_hours=_sunshine_from_condition(next1),
                temperature_celsius=air,
            )
        )
    for row in data.get("dayIntervals", []):
        if not isinstance(row, dict):
            continue
        start_s = row.get("start")
        if not start_s:
            continue
        day = _naive_local(str(start_s)).date()
        if day != target_date:
            continue
        sym_code = str(row.get("twentyFourHourSymbol") or "")
        precip = row.get("precipitation", {})
        precip_val = _float(precip.get("value")) if isinstance(precip, dict) else 0.0
        temp = row.get("temperature", {})
        tmin = _float(temp.get("min")) if isinstance(temp, dict) else None
        tmax = _float(temp.get("max")) if isinstance(temp, dict) else None
        tval = _float(temp.get("value")) if isinstance(temp, dict) else None
        if tmin is None and tmax is None:
            base = tval
        elif tmin is not None and tmax is not None:
            base = (tmin + tmax) / 2.0
        else:
            base = tmax if tmax is not None else tmin
        cond = _canonical_condition(sym_code)
        rain_pct = 100.0 if (precip_val or 0) > 0 else 15.0
        for hour, frac in ((9, 0.35), (14, 0.5), (19, 0.3)):
            tpart = base
            if tmin is not None and tmax is not None:
                tpart = tmin + (tmax - tmin) * frac
            points.append(
                HourlyPoint(
                    local_time=datetime(
                        target_date.year, target_date.month, target_date.day, hour, 0
                    ),
                    condition_summary=cond,
                    precip_probability_pct=rain_pct,
                    sunshine_hours=_sunshine_from_condition(cond),
                    temperature_celsius=round(tpart, 1) if tpart is not None else None,
                )
            )
    return _aggregate_hourly(points, target_date) if points else None
