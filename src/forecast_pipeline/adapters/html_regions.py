"""JSON/HTML slice extraction, text fallback, and WetterOnline structured parse."""

from __future__ import annotations

import json
import re
from datetime import date, timedelta
from html import unescape

from forecast_pipeline.models import DaypartForecast, ForecastDayparts

from forecast_pipeline.config import SourceDefinition
from .html_payloads import (
    DAYPARTS,
    PagePayload,
    _canonical_condition,
    _date_markers,
    _extract_text_window,
    _extract_sunshine_hours,
    _float,
    _sunshine_from_condition,
)


def _dayparts_complete(dayparts: ForecastDayparts) -> bool:
    return all(
        getattr(dayparts, name).condition_summary is not None
        and getattr(dayparts, name).precip_probability_pct is not None
        and getattr(dayparts, name).sunshine_hours is not None
        and getattr(dayparts, name).temperature_celsius is not None
        for name in DAYPARTS
    )


def _fallback_dayparts_from_text(
    definition: SourceDefinition,
    page: PagePayload,
    target_date: date,
) -> ForecastDayparts | None:
    window = _extract_text_window(
        page.text, _date_markers(target_date, definition.language)
    )
    if window is None and target_date <= date.today() + timedelta(days=2):
        window = page.text[:3000]
    if window is None:
        return None
    condition = _canonical_condition(window)
    precip_match = re.search(r"(\d+(?:[.,]\d+)?)\s*%", window)
    precip_probability = _float(precip_match.group(1)) if precip_match else None
    sunshine_total = _extract_sunshine_hours(window)
    if condition is None and precip_probability is None and sunshine_total is None:
        return None
    if condition is None:
        condition = "Regen" if (precip_probability or 0) >= 50 else "Bewölkt"
    sunshine_total = (
        sunshine_total
        if sunshine_total is not None
        else _sunshine_from_condition(condition) * 3
    )
    morning_sun = round(sunshine_total * 0.3, 1)
    afternoon_sun = round(sunshine_total * 0.45, 1)
    evening_sun = round(sunshine_total * 0.25, 1)
    rain = precip_probability if precip_probability is not None else 0.0
    temp_match = re.search(r"(-?\d+(?:[.,]\d+)?)\s*°\s*C", window) or re.search(
        r"(?<!\d)(-?\d+(?:[.,]\d+)?)\s*°(?!\s*C)", window
    )
    temp_val = _float(temp_match.group(1)) if temp_match else None
    return ForecastDayparts(
        morning=DaypartForecast(
            condition_summary=condition,
            precip_probability_pct=rain,
            sunshine_hours=morning_sun,
            temperature_celsius=temp_val,
        ),
        afternoon=DaypartForecast(
            condition_summary=condition,
            precip_probability_pct=rain,
            sunshine_hours=afternoon_sun,
            temperature_celsius=temp_val,
        ),
        evening=DaypartForecast(
            condition_summary=condition,
            precip_probability_pct=rain,
            sunshine_hours=evening_sun,
            temperature_celsius=temp_val,
        ),
    )


def _balanced_segment_from(text: str, anchor_index: int) -> str | None:
    start = -1
    opener = ""
    for candidate in ("[", "{"):
        candidate_index = text.find(candidate, anchor_index)
        if candidate_index != -1 and (start == -1 or candidate_index < start):
            start = candidate_index
            opener = candidate
    if start == -1:
        return None
    closer = "]" if opener == "[" else "}"
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return None


def _balanced_segment(text: str, anchor: str) -> str | None:
    anchor_index = text.find(anchor)
    if anchor_index == -1:
        return None
    return _balanced_segment_from(text, anchor_index)


def _json_segment(text: str, anchor: str) -> object | None:
    segment = _balanced_segment(text, anchor)
    if segment is None:
        return None
    raw = unescape(segment)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        try:
            return json.loads(raw.encode("utf-8").decode("unicode_escape"))
        except json.JSONDecodeError:
            return None


def _json_longest_segment(text: str, anchor: str) -> object | None:
    best: object | None = None
    best_len = -1
    start = 0
    while True:
        anchor_index = text.find(anchor, start)
        if anchor_index == -1:
            break
        segment = _balanced_segment_from(text, anchor_index)
        start = anchor_index + len(anchor)
        if segment is None:
            continue
        raw = unescape(segment)
        for candidate in (raw, raw.encode("utf-8").decode("unicode_escape")):
            try:
                parsed = json.loads(candidate)
            except json.JSONDecodeError:
                continue
            if len(segment) > best_len:
                best = parsed
                best_len = len(segment)
            break
    return best


def _wetteronline_interval_temp(interval: object) -> float | None:
    if not isinstance(interval, dict):
        return None
    precip = interval.get("precipitation")
    if isinstance(precip, dict):
        t = _float(precip.get("temperature"))
        if t is not None:
            return t
    for key in ("temperature", "airTemperature", "temp"):
        block = interval.get(key)
        if isinstance(block, dict):
            t = _float(block.get("value") or block.get("avg") or block.get("max"))
            if t is not None:
                return t
        else:
            t = _float(block)
            if t is not None:
                return t
    return None


def _parse_wetteronline(
    page: PagePayload, target_date: date
) -> ForecastDayparts | None:
    payload = _json_segment(page.html, '"metadata_p_city_local_MediumTerm"')
    if not isinstance(payload, list):
        return None
    match = next(
        (item for item in payload if item.get("date") == target_date.isoformat()), None
    )
    if not isinstance(match, dict):
        return None
    intervals = {item.get("time"): item for item in match.get("intervals", [])}
    sunshine = [
        _float(value) or 0.0 for value in match.get("absoluteSunshineDuration", [])
    ]
    m_iv = intervals.get("morning", {})
    a_iv = intervals.get("afternoon", {})
    e_iv = intervals.get("evening", {})
    return ForecastDayparts(
        morning=DaypartForecast(
            condition_summary=_canonical_condition(
                str(m_iv.get("symbol", "") if isinstance(m_iv, dict) else "")
            ),
            precip_probability_pct=_float(
                m_iv.get("precipitation", {}).get("probability")
                if isinstance(m_iv, dict)
                else None
            ),
            sunshine_hours=round(sum(sunshine[2:4]), 1),
            temperature_celsius=_wetteronline_interval_temp(m_iv),
        ),
        afternoon=DaypartForecast(
            condition_summary=_canonical_condition(
                str(a_iv.get("symbol", "") if isinstance(a_iv, dict) else "")
            ),
            precip_probability_pct=_float(
                a_iv.get("precipitation", {}).get("probability")
                if isinstance(a_iv, dict)
                else None
            ),
            sunshine_hours=round(sum(sunshine[4:6]), 1),
            temperature_celsius=_wetteronline_interval_temp(a_iv),
        ),
        evening=DaypartForecast(
            condition_summary=_canonical_condition(
                str(e_iv.get("symbol", "") if isinstance(e_iv, dict) else "")
            ),
            precip_probability_pct=_float(
                e_iv.get("precipitation", {}).get("probability")
                if isinstance(e_iv, dict)
                else None
            ),
            sunshine_hours=round(sum(sunshine[6:8]), 1),
            temperature_celsius=_wetteronline_interval_temp(e_iv),
        ),
    )
