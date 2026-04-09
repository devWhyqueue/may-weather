from __future__ import annotations

from forecast_pipeline.models import (
    ConsensusForecast,
    DaypartForecast,
    ForecastDayparts,
    SelectedDisplaySource,
    SourceForecast,
)

DAYPARTS = ("morning", "afternoon", "evening")


def _daypart_complete_for_ranking(daypart: DaypartForecast) -> bool:
    return (
        daypart.condition_summary is not None
        and daypart.precip_probability_pct is not None
        and daypart.sunshine_hours is not None
        and daypart.temperature_celsius is not None
    )


def is_ranking_candidate(source: SourceForecast) -> bool:
    """True if this source may compete for the optimistic single-provider display."""

    if not source.ranking_eligible or source.status != "available":
        return False
    return all(
        _daypart_complete_for_ranking(getattr(source.dayparts, name))
        for name in DAYPARTS
    )


def _pending_consensus() -> ConsensusForecast:
    return ConsensusForecast(
        status="pending",
        label="Noch kein vollständiger Tag",
        note="Keine Quelle liefert fuer diesen Tag getrennte Morgen-, Nachmittags- und Abendwerte inkl. Temperatur ohne reine Text-Fallbacks.",
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


def _whole_day_metrics(source: SourceForecast) -> tuple[float, float, float]:
    rains = [
        float(getattr(source.dayparts, n).precip_probability_pct or 0.0)
        for n in DAYPARTS
    ]
    suns = [float(getattr(source.dayparts, n).sunshine_hours or 0.0) for n in DAYPARTS]
    temps = [
        float(getattr(source.dayparts, n).temperature_celsius or 0.0) for n in DAYPARTS
    ]
    return sum(rains) / 3.0, sum(suns), sum(temps) / 3.0


def _optimism_sort_key(
    source: SourceForecast,
) -> tuple[float, float, float, float, str]:
    avg_rain, total_sun, avg_temp = _whole_day_metrics(source)
    return (
        avg_rain,
        -total_sun,
        -avg_temp,
        -source.confidence,
        source.source_id,
    )


def _winner_selection_note(winner: SourceForecast) -> str:
    avg_rain, total_sun, avg_temp = _whole_day_metrics(winner)
    return (
        f"Angezeigt: {winner.source_name} — niedrigster Mittel-Regen ({avg_rain:.1f} %), "
        f"dann meiste Sonnenstunden ({total_sun:.1f} h), dann hoechste Mitteltemperatur ({avg_temp:.1f} °C)."
    )


def _consensus_from_winner(
    winner: SourceForecast,
) -> tuple[ConsensusForecast, SelectedDisplaySource]:
    consensus = ConsensusForecast(
        status="available",
        label="Optimistischste Quelle",
        note=_winner_selection_note(winner),
        source_count=1,
        confidence=round(winner.confidence, 2),
        spread={
            daypart: {"precip_probability_pct": 0.0, "sunshine_hours": 0.0}
            for daypart in DAYPARTS
        },
        dayparts=winner.dayparts,
    )
    selected = SelectedDisplaySource(
        source_id=winner.source_id,
        source_name=winner.source_name,
        source_url=winner.source_url,
    )
    return consensus, selected


def build_optimistic_forecast(
    sources: list[SourceForecast],
) -> tuple[ConsensusForecast, SelectedDisplaySource | None]:
    """Pick the most optimistic provider and expose its dayparts as best_forecast."""

    candidates = [s for s in sources if is_ranking_candidate(s)]
    if not candidates:
        return _pending_consensus(), None
    winner = min(candidates, key=_optimism_sort_key)
    consensus, selected = _consensus_from_winner(winner)
    return consensus, selected
