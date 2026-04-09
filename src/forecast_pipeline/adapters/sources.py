from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html import unescape

import httpx
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from forecast_pipeline.models import DaypartForecast, ForecastDayparts, SourceForecast

from .base import SourceDefinition
from .catalog import SOURCE_DEFINITIONS

DAYPART_HOURS = {
    "morning": range(6, 12),
    "afternoon": range(12, 18),
    "evening": range(18, 24),
}
DAYPARTS = ("morning", "afternoon", "evening")
MONTHS_EN = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
MONTHS_DE = ("Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez")


@dataclass(frozen=True)
class PagePayload:
    html: str
    text: str
    title: str | None
    final_url: str
    status_code: int | None


@dataclass(frozen=True)
class HourlyPoint:
    local_time: datetime
    condition_summary: str | None
    precip_probability_pct: float | None
    sunshine_hours: float | None


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


def _extract_text_window(text: str, markers: tuple[str, ...], window: int = 320) -> str | None:
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
    month = MONTHS_DE[target_date.month - 1] if language == "de" else MONTHS_EN[target_date.month - 1]
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
    if any(token in lowered for token in ("rain", "regen", "drizzle", "sprinkles", "shower")):
        return "Regen"
    if any(token in lowered for token in ("sun", "klar", "clear", "sonnig", "heiter")):
        return "Sonnig"
    if any(token in lowered for token in ("partly", "wechselnd", "wolkig", "teilweise")):
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
    if any(token in lowered for token in ("rain", "regen", "drizzle", "sprinkles", "shower")):
        return 0.0
    if any(token in lowered for token in ("sun", "klar", "clear", "sonnig", "heiter")):
        return 1.0
    if any(token in lowered for token in ("partly", "wechselnd", "wolkig", "teilweise")):
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
            row.condition_summary
            for row in rows
            if row.condition_summary is not None
        ]
        precip_values = [
            row.precip_probability_pct
            for row in rows
            if row.precip_probability_pct is not None
        ]
        sunshine_values = [
            row.sunshine_hours
            for row in rows
            if row.sunshine_hours is not None
        ]
        payload[daypart] = DaypartForecast(
            condition_summary=Counter(conditions).most_common(1)[0][0] if conditions else None,
            precip_probability_pct=round(max(precip_values), 1) if precip_values else None,
            sunshine_hours=round(sum(sunshine_values), 1) if sunshine_values else None,
        )
    return ForecastDayparts(**payload)


def _dayparts_complete(dayparts: ForecastDayparts) -> bool:
    return all(
        getattr(dayparts, name).condition_summary is not None
        and getattr(dayparts, name).precip_probability_pct is not None
        and getattr(dayparts, name).sunshine_hours is not None
        for name in DAYPARTS
    )


def _fallback_dayparts_from_text(
    definition: SourceDefinition,
    page: PagePayload,
    target_date: date,
) -> ForecastDayparts | None:
    window = _extract_text_window(page.text, _date_markers(target_date, definition.language))
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
    sunshine_total = sunshine_total if sunshine_total is not None else _sunshine_from_condition(condition) * 3
    morning_sun = round(sunshine_total * 0.3, 1)
    afternoon_sun = round(sunshine_total * 0.45, 1)
    evening_sun = round(sunshine_total * 0.25, 1)
    rain = precip_probability if precip_probability is not None else 0.0
    return ForecastDayparts(
        morning=DaypartForecast(condition_summary=condition, precip_probability_pct=rain, sunshine_hours=morning_sun),
        afternoon=DaypartForecast(condition_summary=condition, precip_probability_pct=rain, sunshine_hours=afternoon_sun),
        evening=DaypartForecast(condition_summary=condition, precip_probability_pct=rain, sunshine_hours=evening_sun),
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


def _parse_wetteronline(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    payload = _json_segment(page.html, '"metadata_p_city_local_MediumTerm"')
    if not isinstance(payload, list):
        return None
    match = next((item for item in payload if item.get("date") == target_date.isoformat()), None)
    if not isinstance(match, dict):
        return None
    intervals = {item.get("time"): item for item in match.get("intervals", [])}
    sunshine = [_float(value) or 0.0 for value in match.get("absoluteSunshineDuration", [])]
    return ForecastDayparts(
        morning=DaypartForecast(
            condition_summary=_canonical_condition(str(intervals.get("morning", {}).get("symbol", ""))),
            precip_probability_pct=_float(intervals.get("morning", {}).get("precipitation", {}).get("probability")),
            sunshine_hours=round(sum(sunshine[2:4]), 1),
        ),
        afternoon=DaypartForecast(
            condition_summary=_canonical_condition(str(intervals.get("afternoon", {}).get("symbol", ""))),
            precip_probability_pct=_float(intervals.get("afternoon", {}).get("precipitation", {}).get("probability")),
            sunshine_hours=round(sum(sunshine[4:6]), 1),
        ),
        evening=DaypartForecast(
            condition_summary=_canonical_condition(str(intervals.get("evening", {}).get("symbol", ""))),
            precip_probability_pct=_float(intervals.get("evening", {}).get("precipitation", {}).get("probability")),
            sunshine_hours=round(sum(sunshine[6:8]), 1),
        ),
    )


def _parse_timeanddate(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    points: list[HourlyPoint] = []
    day_marker = target_date.strftime("%d. %b")
    pattern = re.compile(
        r'(?P<hour>\d{2}:\d{2})\s+\d+\s*°C\s+(?P<desc>[A-Za-z. ]+)\s+\d+\s*°C\s+\d+\s*km/h\s+\S+\s+\d+%\s+(?P<pc>\d+)%',
        re.IGNORECASE,
    )
    if day_marker not in page.text:
        return None
    for match in pattern.finditer(page.text):
        hour, minute = [int(part) for part in match.group("hour").split(":")]
        condition = _canonical_condition(match.group("desc"))
        points.append(
            HourlyPoint(
                local_time=datetime(target_date.year, target_date.month, target_date.day, hour, minute),
                condition_summary=condition,
                precip_probability_pct=float(match.group("pc")),
                sunshine_hours=_sunshine_from_condition(match.group("desc")),
            )
        )
    return _aggregate_hourly(points, target_date) if points else None


def _parse_weathercom(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    points: list[HourlyPoint] = []
    current_date = date.today()
    previous_hour = -1
    pattern = re.compile(
        r'(?P<hour>\d{2}:\d{2})\s+(?P<cond>[A-Za-zäöüÄÖÜß ]+)\s+\d+\s*°\s+Rain drop\s+(?P<pc>\d+)%.*?UV-Index\s+(?P<uv>\d+)',
        re.IGNORECASE,
    )
    for match in pattern.finditer(page.text):
        hour, minute = [int(part) for part in match.group("hour").split(":")]
        if previous_hour > hour:
            current_date = current_date + timedelta(days=1)
        previous_hour = hour
        condition = _canonical_condition(match.group("cond"))
        points.append(
            HourlyPoint(
                local_time=datetime(current_date.year, current_date.month, current_date.day, hour, minute),
                condition_summary=condition,
                precip_probability_pct=_float(match.group("pc")),
                sunshine_hours=_sunshine_from_condition(
                    match.group("cond"),
                    uv_index=_float(match.group("uv")),
                ),
            )
        )
    return _aggregate_hourly(points, target_date) if points else None


def _parse_yr(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    points: list[HourlyPoint] = []
    pattern = re.compile(
        r'\{\\"symbol\\":\{.*?\\"symbolCode\\":\{\\"next1Hour\\":\\"(?P<symbol>[^"]+)\\".*?\\"precipitation\\":\{\\"value\\":(?P<precip>[\d.]+)\}.*?\\"uvIndex\\":\{\\"value\\":(?P<uv>[\d.]+)\}.*?\\"cloudCover\\":\{\\"value\\":(?P<cloud>[\d.]+).*?\\"start\\":\\"(?P<start>[^"]+)\\"',
    )
    for match in pattern.finditer(page.html):
        local_time = datetime.fromisoformat(match.group("start"))
        condition = _canonical_condition(match.group("symbol"))
        precip_amount = _float(match.group("precip")) or 0.0
        points.append(
            HourlyPoint(
                local_time=local_time.replace(tzinfo=None),
                condition_summary=condition,
                precip_probability_pct=100.0 if precip_amount > 0 else 0.0,
                sunshine_hours=_sunshine_from_condition(
                    match.group("symbol"),
                    uv_index=_float(match.group("uv")),
                    cloud_cover=_float(match.group("cloud")),
                ),
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


def _parse_weatherandradar(page: PagePayload, target_date: date) -> ForecastDayparts | None:
    summary_match = re.search(
        r"Morning\s+\d+\s*°\s+(?P<morning>\d+)\s*%\s+Afternoon\s+\d+\s*°\s+(?P<afternoon>\d+)\s*%\s+Evening\s+\d+\s*°\s+(?P<evening>\d+)\s*%",
        page.text,
    )
    sunshine_match = re.search(r'aria-label="(?P<hours>\d+)hours of sunshine"', page.html)
    if not summary_match:
        return None
    sunshine_total = _float(sunshine_match.group("hours")) if sunshine_match else 3.0
    sunshine_total = sunshine_total or 3.0
    return ForecastDayparts(
        morning=DaypartForecast(
            condition_summary="Bewölkt",
            precip_probability_pct=_float(summary_match.group("morning")),
            sunshine_hours=round(sunshine_total * 0.3, 1),
        ),
        afternoon=DaypartForecast(
            condition_summary="Wolkig",
            precip_probability_pct=_float(summary_match.group("afternoon")),
            sunshine_hours=round(sunshine_total * 0.45, 1),
        ),
        evening=DaypartForecast(
            condition_summary="Bewölkt",
            precip_probability_pct=_float(summary_match.group("evening")),
            sunshine_hours=round(sunshine_total * 0.25, 1),
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
    target_label = "morgen" if target_date > date.today() else None
    header_cells = headers if len(headers) == len(cells) else headers[1:]
    for header, cell in zip(header_cells, cells, strict=False):
        header_text = _normalize_whitespace(header.get_text(" ", strip=True))
        if target_label and target_label not in header_text.lower():
            continue
        time_match = re.search(r"(\d{2}):(\d{2})", header_text)
        if not time_match:
            continue
        hour = int(time_match.group(1))
        minute = int(time_match.group(2))
        img = cell.find("img")
        condition_raw = str(img.get("alt")) if img and img.get("alt") is not None else None
        prob = None
        prob_node = cell.find("span", class_="prob-line")
        if prob_node:
            prob = _float(prob_node.get_text(" ", strip=True))
        points.append(
            HourlyPoint(
                local_time=datetime(target_date.year, target_date.month, target_date.day, hour, minute),
                condition_summary=_canonical_condition(condition_raw),
                precip_probability_pct=prob,
                sunshine_hours=_sunshine_from_condition(condition_raw),
            )
        )
    return _aggregate_hourly(points, target_date) if points else None


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
        for valid_time, icon, probability in zip(times, icons, probs, strict=False):
            local_time = datetime.fromisoformat(str(valid_time))
            points.append(
                HourlyPoint(
                    local_time=local_time.replace(tzinfo=None),
                    condition_summary=_canonical_condition(str(icon) if icon is not None else None),
                    precip_probability_pct=probability,
                    sunshine_hours=_sunshine_from_condition(str(icon) if icon is not None else None),
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
        r'(?P<hour>\d{2}:\d{2})\s+(?:(?P<prob>\d+)%\s+(?:[\d.,]+\s*mm)\s+)?(?P<temp>\d+)°\s+(?P<cond>[A-Za-zäöüÄÖÜß ]+?)\s+gefühlte',
        re.IGNORECASE,
    )
    for match in pattern.finditer(text):
        hour, minute = [int(part) for part in match.group("hour").split(":")]
        if hour >= 24:
            continue
        condition = _canonical_condition(match.group("cond"))
        points.append(
            HourlyPoint(
                local_time=datetime(target_date.year, target_date.month, target_date.day, hour, minute),
                condition_summary=condition,
                precip_probability_pct=_float(match.group("prob")) or 0.0,
                sunshine_hours=_sunshine_from_condition(match.group("cond")),
            )
        )
    return _aggregate_hourly(points, target_date) if points else None


_SOURCE_PARSERS = {
    "wetteronline": _parse_wetteronline,
    "meteoblue": _parse_meteoblue,
    "daswetter": _parse_daswetter,
    "timeanddate": _parse_timeanddate,
    "weathercom": _parse_weathercom,
    "yr": _parse_yr,
    "foreca": _parse_foreca,
    "msn": _parse_msn,
    "weatherandradar": _parse_weatherandradar,
    "ventusky": _parse_ventusky,
}


def _empty_result(
    definition: SourceDefinition,
    *,
    fetched_at: str,
    target_date: date,
    status: str,
    note: str,
) -> SourceForecast:
    return SourceForecast(
        source_id=definition.source_id,
        source_name=definition.source_name,
        fetched_at=fetched_at,
        target_date=target_date.isoformat(),
        source_url=definition.source_url,
        method=definition.method,
        confidence=0.0,
        status=status,
        note=note,
        dayparts=_empty_dayparts(),
    )


def _successful_result(
    definition: SourceDefinition,
    *,
    fetched_at: str,
    target_date: date,
    dayparts: ForecastDayparts,
) -> SourceForecast:
    complete = _dayparts_complete(dayparts)
    return SourceForecast(
        source_id=definition.source_id,
        source_name=definition.source_name,
        fetched_at=fetched_at,
        target_date=target_date.isoformat(),
        source_url=definition.source_url,
        method=definition.method,
        confidence=min(1.0, definition.weight / 1.1) if complete else 0.45,
        status="available" if complete else "partial",
        note=None if complete else "Ein oder mehrere Tagesabschnitte konnten nur teilweise extrahiert werden.",
        dayparts=dayparts,
    )


class BaseSourceAdapter:
    def __init__(self, definition: SourceDefinition, *, headed: bool = False) -> None:
        self.definition = definition
        self.headed = headed

    def page_to_result(
        self,
        page: PagePayload,
        *,
        fetched_at: str,
        target_date: date,
    ) -> SourceForecast:
        if page.status_code and page.status_code >= 400:
            return _empty_result(
                self.definition,
                fetched_at=fetched_at,
                target_date=target_date,
                status="unavailable",
                note=f"Quelle liefert keine nutzbare Vorhersageseite ({page.status_code}).",
            )
        combined = _normalize_whitespace(f"{page.title or ''} {page.text} {page.final_url}")
        lowered = combined.lower()
        normalized = lowered.replace("-", " ")
        invalid_match = next(
            (marker for marker in self.definition.invalid_markers if marker.lower() in lowered),
            None,
        )
        if invalid_match:
            return _empty_result(
                self.definition,
                fetched_at=fetched_at,
                target_date=target_date,
                status="unavailable",
                note=f"Quelle liefert keine nutzbare Vorhersageseite ({invalid_match}).",
            )
        if self.definition.location_markers and not any(
            marker.lower() in normalized for marker in self.definition.location_markers
        ):
            return _empty_result(
                self.definition,
                fetched_at=fetched_at,
                target_date=target_date,
                status="unavailable",
                note="Quelle zeigt keine Vorhersageseite fuer Haltern am See.",
            )
        parser = _SOURCE_PARSERS.get(self.definition.source_id)
        if parser is None:
            return _empty_result(
                self.definition,
                fetched_at=fetched_at,
                target_date=target_date,
                status="error",
                note="Kein Parser fuer diese Quelle hinterlegt.",
            )
        dayparts = parser(page, target_date)
        fallback = _fallback_dayparts_from_text(self.definition, page, target_date)
        if dayparts is None:
            dayparts = fallback
        elif fallback is not None:
            merged: dict[str, DaypartForecast] = {}
            for name in DAYPARTS:
                primary = getattr(dayparts, name)
                backup = getattr(fallback, name)
                merged[name] = DaypartForecast(
                    condition_summary=primary.condition_summary or backup.condition_summary,
                    precip_probability_pct=primary.precip_probability_pct if primary.precip_probability_pct is not None else backup.precip_probability_pct,
                    sunshine_hours=primary.sunshine_hours if primary.sunshine_hours is not None else backup.sunshine_hours,
                )
            dayparts = ForecastDayparts(**merged)
        if dayparts is None:
            return _empty_result(
                self.definition,
                fetched_at=fetched_at,
                target_date=target_date,
                status="unavailable",
                note="Die Quelle veroeffentlicht fuer diesen Tag noch keinen vollstaendigen Tagesabschnitt.",
            )
        return _successful_result(
            self.definition,
            fetched_at=fetched_at,
            target_date=target_date,
            dayparts=dayparts,
        )

    def error_result(
        self,
        *,
        fetched_at: str,
        target_date: date,
        exc: Exception,
    ) -> SourceForecast:
        return _empty_result(
            self.definition,
            fetched_at=fetched_at,
            target_date=target_date,
            status="error",
            note=f"Quelle konnte nicht verarbeitet werden: {exc.__class__.__name__}",
        )

    def fetch(
        self,
        client: httpx.Client,
        *,
        target_date: date,
        fetched_at: str,
    ) -> SourceForecast:
        try:
            payload = self.load_page(client)
        except Exception as exc:
            return self.error_result(fetched_at=fetched_at, target_date=target_date, exc=exc)
        return self.page_to_result(payload, fetched_at=fetched_at, target_date=target_date)

    def load_page(self, client: httpx.Client) -> PagePayload:
        return self._fetch_page(client)

    def _fetch_page(self, client: httpx.Client) -> PagePayload:
        raise NotImplementedError


class HttpSourceAdapter(BaseSourceAdapter):
    def _fetch_page(self, client: httpx.Client) -> PagePayload:
        response = client.get(self.definition.source_url, follow_redirects=True)
        html = response.text
        return PagePayload(
            html=html,
            text=_strip_html(html),
            title=_extract_title(html),
            final_url=str(response.url),
            status_code=response.status_code,
        )


class PlaywrightSourceAdapter(BaseSourceAdapter):
    def _fetch_page(self, client: httpx.Client) -> PagePayload:
        del client
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=not self.headed)
            page = browser.new_page(
                locale="de-DE",
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
            )
            response = page.goto(self.definition.source_url, wait_until="domcontentloaded")
            if self.definition.wait_for_ms:
                page.wait_for_timeout(self.definition.wait_for_ms)
            html = page.content()
            text = _normalize_whitespace(page.locator("body").inner_text())
            title = page.title()
            final_url = page.url
            status_code = response.status if response else None
            browser.close()
        return PagePayload(
            html=html,
            text=text,
            title=title,
            final_url=final_url,
            status_code=status_code,
        )


def build_source_adapters(*, headed: bool = False) -> list[BaseSourceAdapter]:
    adapters: list[BaseSourceAdapter] = []
    for definition in SOURCE_DEFINITIONS:
        if definition.fetch_mode == "playwright":
            adapters.append(PlaywrightSourceAdapter(definition, headed=headed))
        else:
            adapters.append(HttpSourceAdapter(definition, headed=headed))
    return adapters
