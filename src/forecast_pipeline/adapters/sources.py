from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Callable

import httpx
from bs4 import BeautifulSoup

from forecast_pipeline.models import ForecastMetrics, SourceForecast
from .base import SourceDefinition
from .catalog import SOURCE_DEFINITIONS


def _date_markers(target_date: date) -> tuple[str, ...]:
    day_no_pad = f"{target_date.day}.{target_date.month}.{target_date.year}"
    return (
        target_date.isoformat(),
        target_date.strftime("%d.%m.%Y"),
        day_no_pad,
        target_date.strftime("%A").lower(),
        target_date.strftime("%a").lower(),
    )


def _extract_title(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    if soup.title and soup.title.string:
        return " ".join(soup.title.string.split())
    return None


def _extract_text_window(html: str, marker: str, window: int = 360) -> str | None:
    lowered = html.lower()
    index = lowered.find(marker.lower())
    if index == -1:
        return None
    start = max(0, index - window)
    end = min(len(html), index + window)
    return html[start:end]


def _extract_first_number(pattern: str, text: str) -> float | None:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", "."))
    except ValueError:
        return None


def _extract_metrics(text: str) -> ForecastMetrics:
    condition_match = re.search(
        r"(sonnig|heiter|bewölkt|wolkig|regen|schauer|schnee|klar|partly cloudy|overcast|sunny)",
        text,
        flags=re.IGNORECASE,
    )
    return ForecastMetrics(
        temp_min_c=_extract_first_number(
            r"(?:min|tiefst|minimum)[^0-9-]{0,24}(-?\d+(?:[.,]\d+)?)\s*°", text
        ),
        temp_max_c=_extract_first_number(
            r"(?:max|höchst|maximum)[^0-9-]{0,24}(-?\d+(?:[.,]\d+)?)\s*°", text
        ),
        precip_probability_pct=_extract_first_number(r"(\d+(?:[.,]\d+)?)\s*%", text),
        precip_mm=_extract_first_number(r"(\d+(?:[.,]\d+)?)\s*mm", text),
        wind_kph=_extract_first_number(r"(\d+(?:[.,]\d+)?)\s*(?:km/h|kph)", text),
        condition_summary=condition_match.group(1).capitalize()
        if condition_match
        else None,
    )


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
        metrics=ForecastMetrics(),
    )


def _fetch_html(client: httpx.Client, request_url: str) -> str:
    response = client.get(request_url, follow_redirects=True)
    response.raise_for_status()
    return response.text


def _unavailable_note(definition: SourceDefinition, html: str) -> str:
    title = _extract_title(html) or "Kein Datums-Hinweis"
    if definition.horizon_days is None:
        return f"Zieltermin noch nicht im veröffentlichten Vorhersagefenster. Seitenkopf: {title}"
    return (
        f"Zieltermin liegt vermutlich noch außerhalb des typischen {definition.horizon_days}-Tage-Fensters. "
        f"Seitenkopf: {title}"
    )


def _target_window(html: str, target_date: date) -> str:
    for marker in _date_markers(target_date):
        window = _extract_text_window(html, marker)
        if window:
            return window
    return html


def _successful_result(
    definition: SourceDefinition,
    *,
    fetched_at: str,
    target_date: date,
    metrics: ForecastMetrics,
) -> SourceForecast:
    usable = any(value is not None for value in metrics.__dict__.values())
    return SourceForecast(
        source_id=definition.source_id,
        source_name=definition.source_name,
        fetched_at=fetched_at,
        target_date=target_date.isoformat(),
        source_url=definition.source_url,
        method=definition.method,
        confidence=min(1.0, definition.weight / 1.4) if usable else 0.35,
        status="available" if usable else "partial",
        note=None
        if usable
        else "Datum gefunden, aber nur teilweise strukturierte Daten extrahierbar.",
        metrics=metrics,
    )


def _html_or_error(
    adapter: "HtmlDateAdapter",
    client: httpx.Client,
    *,
    fetched_at: str,
    target_date: date,
) -> str | SourceForecast:
    try:
        return _fetch_html(client, adapter.request_url)
    except httpx.HTTPError as exc:
        return _empty_result(
            adapter.definition,
            fetched_at=fetched_at,
            target_date=target_date,
            status="error",
            note=f"Quelle nicht erreichbar: {exc.__class__.__name__}",
        )


def _gated_result(
    adapter: "HtmlDateAdapter", html: str, *, fetched_at: str, target_date: date
) -> SourceForecast | None:
    gate = adapter.date_gate or adapter._default_gate
    if gate(html, target_date):
        return None
    return _empty_result(
        adapter.definition,
        fetched_at=fetched_at,
        target_date=target_date,
        status="unavailable",
        note=_unavailable_note(adapter.definition, html),
    )


def _validated_html(
    adapter: "HtmlDateAdapter",
    client: httpx.Client,
    *,
    fetched_at: str,
    target_date: date,
) -> str | SourceForecast:
    html_or_result = _html_or_error(
        adapter,
        client,
        fetched_at=fetched_at,
        target_date=target_date,
    )
    if isinstance(html_or_result, SourceForecast):
        return html_or_result
    return (
        _gated_result(
            adapter,
            html_or_result,
            fetched_at=fetched_at,
            target_date=target_date,
        )
        or html_or_result
    )


@dataclass(frozen=True)
class HtmlDateAdapter:
    """HTTP adapter that searches provider HTML for the target date and nearby values."""

    definition: SourceDefinition
    request_url: str
    date_gate: Callable[[str, date], bool] | None = None

    def fetch(
        self,
        client: httpx.Client,
        *,
        target_date: date,
        fetched_at: str,
    ) -> SourceForecast:
        """Fetch one source and normalize the published result."""

        html_or_result = _validated_html(
            self,
            client,
            fetched_at=fetched_at,
            target_date=target_date,
        )
        if isinstance(html_or_result, SourceForecast):
            return html_or_result
        metrics = _extract_metrics(_target_window(html_or_result, target_date))
        return _successful_result(
            self.definition,
            fetched_at=fetched_at,
            target_date=target_date,
            metrics=metrics,
        )

    @staticmethod
    def _default_gate(html: str, target_date: date) -> bool:
        lowered = html.lower()
        return any(marker.lower() in lowered for marker in _date_markers(target_date))


def build_source_adapters() -> list[HtmlDateAdapter]:
    """Build the adapter list for the configured ten German-facing weather services."""
    return [
        HtmlDateAdapter(definition=definition, request_url=definition.source_url)
        for definition in SOURCE_DEFINITIONS
    ]
