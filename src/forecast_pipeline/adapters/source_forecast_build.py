"""Build `SourceForecast` rows from fetched pages, parsers, and text fallbacks."""

from __future__ import annotations

from datetime import date

from forecast_pipeline.models import DaypartForecast, ForecastDayparts, SourceForecast

from forecast_pipeline.config import SourceDefinition
from .html_payloads import (
    DAYPARTS,
    PagePayload,
    _empty_dayparts,
    _normalize_whitespace,
)
from .html_regions import (
    _dayparts_complete,
    _fallback_dayparts_from_text,
)
from .parsers_remote_b import SOURCE_PARSERS


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
        ranking_eligible=False,
    )


def _successful_result(
    definition: SourceDefinition,
    *,
    fetched_at: str,
    target_date: date,
    dayparts: ForecastDayparts,
    ranking_eligible: bool,
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
        note=None
        if complete
        else "Ein oder mehrere Tagesabschnitte konnten nur teilweise extrahiert werden.",
        dayparts=dayparts,
        ranking_eligible=ranking_eligible and complete,
    )


def _maybe_http_unavailable(
    definition: SourceDefinition,
    page: PagePayload,
    *,
    fetched_at: str,
    target_date: date,
) -> SourceForecast | None:
    if page.status_code and page.status_code >= 400:
        return _empty_result(
            definition,
            fetched_at=fetched_at,
            target_date=target_date,
            status="unavailable",
            note=f"Quelle liefert keine nutzbare Vorhersageseite ({page.status_code}).",
        )
    return None


def _maybe_invalid_content(
    definition: SourceDefinition,
    page: PagePayload,
    *,
    fetched_at: str,
    target_date: date,
) -> SourceForecast | None:
    combined = _normalize_whitespace(f"{page.title or ''} {page.text} {page.final_url}")
    lowered = combined.lower()
    invalid_match = next(
        (marker for marker in definition.invalid_markers if marker.lower() in lowered),
        None,
    )
    if invalid_match:
        return _empty_result(
            definition,
            fetched_at=fetched_at,
            target_date=target_date,
            status="unavailable",
            note=f"Quelle liefert keine nutzbare Vorhersageseite ({invalid_match}).",
        )
    return None


def _maybe_wrong_location(
    definition: SourceDefinition,
    page: PagePayload,
    *,
    fetched_at: str,
    target_date: date,
) -> SourceForecast | None:
    combined = _normalize_whitespace(f"{page.title or ''} {page.text} {page.final_url}")
    normalized = combined.lower().replace("-", " ")
    if definition.location_markers and not any(
        marker.lower() in normalized for marker in definition.location_markers
    ):
        return _empty_result(
            definition,
            fetched_at=fetched_at,
            target_date=target_date,
            status="unavailable",
            note="Quelle zeigt keine Vorhersageseite fuer Haltern am See.",
        )
    return None


def _merge_primary_and_fallback(
    primary: ForecastDayparts, fallback: ForecastDayparts
) -> ForecastDayparts:
    merged: dict[str, DaypartForecast] = {}
    for name in DAYPARTS:
        p = getattr(primary, name)
        b = getattr(fallback, name)
        merged[name] = DaypartForecast(
            condition_summary=p.condition_summary or b.condition_summary,
            precip_probability_pct=p.precip_probability_pct
            if p.precip_probability_pct is not None
            else b.precip_probability_pct,
            sunshine_hours=p.sunshine_hours
            if p.sunshine_hours is not None
            else b.sunshine_hours,
            temperature_celsius=p.temperature_celsius
            if p.temperature_celsius is not None
            else b.temperature_celsius,
        )
    return ForecastDayparts(**merged)


def _dayparts_after_parse(
    definition: SourceDefinition,
    page: PagePayload,
    target_date: date,
) -> tuple[ForecastDayparts | None, bool] | None:
    parse_fn = SOURCE_PARSERS.get(definition.source_id)
    if parse_fn is None:
        return None
    primary = parse_fn(page, target_date)
    fallback = _fallback_dayparts_from_text(definition, page, target_date)
    ranking_eligible = primary is not None
    if primary is None:
        merged = fallback
    elif fallback is not None:
        merged = _merge_primary_and_fallback(primary, fallback)
    else:
        merged = primary
    return merged, ranking_eligible


def _parsed_forecast_or_empty(
    definition: SourceDefinition,
    page: PagePayload,
    *,
    fetched_at: str,
    target_date: date,
) -> SourceForecast:
    parsed = _dayparts_after_parse(definition, page, target_date)
    if parsed is None:
        return _empty_result(
            definition,
            fetched_at=fetched_at,
            target_date=target_date,
            status="error",
            note="Kein Parser fuer diese Quelle hinterlegt.",
        )
    dayparts, ranking_eligible = parsed
    if dayparts is None:
        return _empty_result(
            definition,
            fetched_at=fetched_at,
            target_date=target_date,
            status="unavailable",
            note="Die Quelle veroeffentlicht fuer diesen Tag noch keinen vollstaendigen Tagesabschnitt.",
        )
    return _successful_result(
        definition,
        fetched_at=fetched_at,
        target_date=target_date,
        dayparts=dayparts,
        ranking_eligible=ranking_eligible,
    )
