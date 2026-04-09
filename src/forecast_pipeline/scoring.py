from __future__ import annotations

from statistics import mean

from forecast_pipeline.models import (
    ConsensusForecast,
    DaypartForecast,
    ForecastDayparts,
    SourceForecast,
)

DAYPARTS = ("morning", "afternoon", "evening")


def _daypart_complete(daypart: DaypartForecast) -> bool:
    return (
        daypart.condition_summary is not None
        and daypart.precip_probability_pct is not None
        and daypart.sunshine_hours is not None
    )


def _has_signal(source: SourceForecast) -> bool:
    return all(_daypart_complete(getattr(source.dayparts, name)) for name in DAYPARTS)


def _weighted_average(values: list[tuple[float, float]]) -> float | None:
    if not values:
        return None
    numerator = sum(value * weight for value, weight in values)
    denominator = sum(weight for _, weight in values)
    if denominator == 0:
        return None
    return round(numerator / denominator, 1)


def _spread(values: list[float]) -> float | None:
    if len(values) < 2:
        return 0.0 if values else None
    return round(max(values) - min(values), 1)


def _available_sources(sources: list[SourceForecast]) -> list[SourceForecast]:
    return [source for source in sources if source.status == "available" and _has_signal(source)]


def _weighted_series(
    sources: list[SourceForecast],
    daypart_name: str,
    metric_name: str,
) -> list[tuple[float, float]]:
    values: list[tuple[float, float]] = []
    for source in sources:
        daypart = getattr(source.dayparts, daypart_name)
        value = getattr(daypart, metric_name)
        if value is not None:
            values.append((value, source.confidence))
    return values


def _pending_consensus() -> ConsensusForecast:
    return ConsensusForecast(
        status="pending",
        label="Noch kein vollständiger Tag",
        note="Die abgefragten Quellen liefern noch keinen vollstaendigen Morgen-, Nachmittags- und Abendblock.",
        source_count=0,
        confidence=0.0,
        spread={
            daypart: {
                "precip_probability_pct": None,
                "sunshine_hours": None,
            }
            for daypart in DAYPARTS
        },
        dayparts=ForecastDayparts(),
    )


def _spread_payload(
    weighted_values: dict[str, dict[str, list[tuple[float, float]]]],
) -> dict[str, dict[str, float | None]]:
    return {
        daypart: {
            "precip_probability_pct": _spread(
                [value for value, _ in weighted_values[daypart]["precip_probability_pct"]]
            ),
            "sunshine_hours": _spread(
                [value for value, _ in weighted_values[daypart]["sunshine_hours"]]
            ),
        }
        for daypart in DAYPARTS
    }


def _confidence_for(
    sources: list[SourceForecast],
    spread: dict[str, dict[str, float | None]],
) -> float:
    mean_confidence = mean(source.confidence for source in sources)
    completeness = min(1.0, len(sources) / 10)
    stability_penalty = 0.0
    for daypart in DAYPARTS:
        precip_spread = spread[daypart]["precip_probability_pct"]
        sunshine_spread = spread[daypart]["sunshine_hours"]
        if precip_spread not in (None, 0.0):
            stability_penalty += min(0.12, (precip_spread or 0.0) / 100)
        if sunshine_spread not in (None, 0.0):
            stability_penalty += min(0.12, (sunshine_spread or 0.0) / 8)
    return round(
        max(0.0, min(1.0, (mean_confidence * 0.7 + completeness * 0.3) - stability_penalty)),
        2,
    )


def _condition_summary(sources: list[SourceForecast], daypart_name: str) -> str | None:
    votes = [
        getattr(source.dayparts, daypart_name).condition_summary
        for source in sources
        if getattr(source.dayparts, daypart_name).condition_summary
    ]
    return max(set(votes), key=votes.count) if votes else None


def _weighted_values(
    sources: list[SourceForecast],
) -> dict[str, dict[str, list[tuple[float, float]]]]:
    return {
        daypart: {
            "precip_probability_pct": _weighted_series(
                sources,
                daypart,
                "precip_probability_pct",
            ),
            "sunshine_hours": _weighted_series(
                sources,
                daypart,
                "sunshine_hours",
            ),
        }
        for daypart in DAYPARTS
    }


def _consensus_dayparts(
    weighted_values: dict[str, dict[str, list[tuple[float, float]]]],
    sources: list[SourceForecast],
) -> ForecastDayparts:
    return ForecastDayparts(
        morning=DaypartForecast(
            condition_summary=_condition_summary(sources, "morning"),
            precip_probability_pct=_weighted_average(
                weighted_values["morning"]["precip_probability_pct"]
            ),
            sunshine_hours=_weighted_average(weighted_values["morning"]["sunshine_hours"]),
        ),
        afternoon=DaypartForecast(
            condition_summary=_condition_summary(sources, "afternoon"),
            precip_probability_pct=_weighted_average(
                weighted_values["afternoon"]["precip_probability_pct"]
            ),
            sunshine_hours=_weighted_average(weighted_values["afternoon"]["sunshine_hours"]),
        ),
        evening=DaypartForecast(
            condition_summary=_condition_summary(sources, "evening"),
            precip_probability_pct=_weighted_average(
                weighted_values["evening"]["precip_probability_pct"]
            ),
            sunshine_hours=_weighted_average(weighted_values["evening"]["sunshine_hours"]),
        ),
    )


def build_consensus(sources: list[SourceForecast]) -> ConsensusForecast:
    available_sources = _available_sources(sources)
    if not available_sources:
        return _pending_consensus()

    weighted_values = _weighted_values(available_sources)
    spread = _spread_payload(weighted_values)
    return ConsensusForecast(
        status="available",
        label="Verdichtete Tagesabschnitte",
        note=f"{len(available_sources)} Quellen liefern vollstaendige Morgen-, Nachmittags- und Abendwerte.",
        source_count=len(available_sources),
        confidence=_confidence_for(available_sources, spread),
        spread=spread,
        dayparts=_consensus_dayparts(weighted_values, available_sources),
    )
