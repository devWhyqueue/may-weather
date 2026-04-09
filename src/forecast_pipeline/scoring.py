from __future__ import annotations

from statistics import mean

from forecast_pipeline.models import ConsensusForecast, ForecastMetrics, SourceForecast


def _has_signal(source: SourceForecast) -> bool:
    metrics = source.metrics
    return any(
        value is not None
        for value in (
            metrics.temp_min_c,
            metrics.temp_max_c,
            metrics.precip_probability_pct,
            metrics.precip_mm,
            metrics.wind_kph,
            metrics.condition_summary,
        )
    )


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
    return [
        source
        for source in sources
        if source.status in {"available", "partial"} and _has_signal(source)
    ]


def _weighted_series(
    sources: list[SourceForecast], metric_name: str
) -> list[tuple[float, float]]:
    pairs: list[tuple[float, float]] = []
    for source in sources:
        value = getattr(source.metrics, metric_name)
        if value is not None:
            pairs.append((value, source.confidence))
    return pairs


def _pending_consensus() -> ConsensusForecast:
    return ConsensusForecast(
        status="pending",
        label="Noch kein belastbarer Konsens",
        note="Der Zieltermin liegt bei den abgefragten Wetterdiensten aktuell noch außerhalb des veröffentlichten Vorhersagefensters.",
        source_count=0,
        confidence=0.0,
        spread={
            "temp_max_c": None,
            "temp_min_c": None,
            "precip_probability_pct": None,
            "wind_kph": None,
        },
        metrics=ForecastMetrics(),
    )


def _spread_payload(
    weighted_values: dict[str, list[tuple[float, float]]],
) -> dict[str, float | None]:
    return {
        "temp_max_c": _spread([value for value, _ in weighted_values["temp_max_c"]]),
        "temp_min_c": _spread([value for value, _ in weighted_values["temp_min_c"]]),
        "precip_probability_pct": _spread(
            [value for value, _ in weighted_values["precip_probability_pct"]]
        ),
        "wind_kph": _spread([value for value, _ in weighted_values["wind_kph"]]),
    }


def _confidence_for(
    sources: list[SourceForecast], spread: dict[str, float | None]
) -> float:
    mean_confidence = mean(source.confidence for source in sources)
    completeness = min(1.0, len(sources) / 5)
    stability_penalty = 0.0
    if spread["temp_max_c"] not in (None, 0.0):
        stability_penalty += min(0.25, (spread["temp_max_c"] or 0.0) / 40)
    if spread["precip_probability_pct"] not in (None, 0.0):
        stability_penalty += min(0.25, (spread["precip_probability_pct"] or 0.0) / 100)
    return round(
        max(
            0.0,
            min(1.0, (mean_confidence * 0.7 + completeness * 0.3) - stability_penalty),
        ),
        2,
    )


def _condition_summary(sources: list[SourceForecast]) -> str | None:
    votes = [
        source.metrics.condition_summary
        for source in sources
        if source.metrics.condition_summary
    ]
    return max(set(votes), key=votes.count) if votes else None


def _weighted_values(
    sources: list[SourceForecast],
) -> dict[str, list[tuple[float, float]]]:
    return {
        "temp_max_c": _weighted_series(sources, "temp_max_c"),
        "temp_min_c": _weighted_series(sources, "temp_min_c"),
        "precip_probability_pct": _weighted_series(sources, "precip_probability_pct"),
        "precip_mm": _weighted_series(sources, "precip_mm"),
        "wind_kph": _weighted_series(sources, "wind_kph"),
    }


def _consensus_metrics(
    weighted_values: dict[str, list[tuple[float, float]]], sources: list[SourceForecast]
) -> ForecastMetrics:
    return ForecastMetrics(
        temp_min_c=_weighted_average(weighted_values["temp_min_c"]),
        temp_max_c=_weighted_average(weighted_values["temp_max_c"]),
        precip_probability_pct=_weighted_average(
            weighted_values["precip_probability_pct"]
        ),
        precip_mm=_weighted_average(weighted_values["precip_mm"]),
        wind_kph=_weighted_average(weighted_values["wind_kph"]),
        condition_summary=_condition_summary(sources),
    )


def build_consensus(sources: list[SourceForecast]) -> ConsensusForecast:
    """Build a weighted summary forecast from all currently usable source records."""

    available_sources = _available_sources(sources)
    if not available_sources:
        return _pending_consensus()

    weighted_values = _weighted_values(available_sources)
    spread = _spread_payload(weighted_values)
    return ConsensusForecast(
        status="available",
        label="Verdichtete Prognose",
        note=f"{len(available_sources)} Quellen liefern bereits Werte für den Zieltermin.",
        source_count=len(available_sources),
        confidence=_confidence_for(available_sources, spread),
        spread=spread,
        metrics=_consensus_metrics(weighted_values, available_sources),
    )
