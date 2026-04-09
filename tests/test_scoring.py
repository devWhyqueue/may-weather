from forecast_pipeline.models import DaypartForecast, ForecastDayparts, SourceForecast
from forecast_pipeline.scoring import build_optimistic_forecast, is_ranking_candidate


def _dp(
    *,
    condition: str = "Wolkig",
    rain: float = 30.0,
    sun: float = 2.0,
    temp: float = 15.0,
) -> DaypartForecast:
    return DaypartForecast(
        condition_summary=condition,
        precip_probability_pct=rain,
        sunshine_hours=sun,
        temperature_celsius=temp,
    )


def make_source(
    *,
    source_id: str,
    confidence: float,
    morning: DaypartForecast,
    afternoon: DaypartForecast,
    evening: DaypartForecast,
    status: str = "available",
    ranking_eligible: bool = True,
) -> SourceForecast:
    dayparts = ForecastDayparts(morning=morning, afternoon=afternoon, evening=evening)
    return SourceForecast(
        source_id=source_id,
        source_name=source_id,
        fetched_at="2026-04-09T12:00:00Z",
        target_date="2026-04-10",
        source_url="https://example.com",
        method="html",
        confidence=confidence,
        status=status,
        note=None,
        dayparts=dayparts,
        ranking_eligible=ranking_eligible,
    )


def test_optimistic_prefers_lowest_mean_rain() -> None:
    wetter = _dp(rain=40, sun=3, temp=18)
    dry = _dp(rain=10, sun=3, temp=18)
    consensus, selected = build_optimistic_forecast(
        [
            make_source(source_id="a", confidence=0.9, morning=wetter, afternoon=wetter, evening=wetter),
            make_source(source_id="b", confidence=0.5, morning=dry, afternoon=dry, evening=dry),
        ]
    )
    assert consensus.status == "available"
    assert selected is not None
    assert selected.source_id == "b"


def test_optimistic_rain_tie_prefers_more_sunshine() -> None:
    low_sun = _dp(rain=20, sun=1.0, temp=20)
    high_sun = _dp(rain=20, sun=4.0, temp=20)
    consensus, selected = build_optimistic_forecast(
        [
            make_source(source_id="x", confidence=0.9, morning=low_sun, afternoon=low_sun, evening=low_sun),
            make_source(source_id="y", confidence=0.9, morning=high_sun, afternoon=high_sun, evening=high_sun),
        ]
    )
    assert selected is not None
    assert selected.source_id == "y"


def test_optimistic_rain_and_sun_tie_prefers_higher_temperature() -> None:
    cool = _dp(rain=15, sun=2.0, temp=10)
    warm = _dp(rain=15, sun=2.0, temp=22)
    _, selected = build_optimistic_forecast(
        [
            make_source(source_id="cool", confidence=0.9, morning=cool, afternoon=cool, evening=cool),
            make_source(source_id="warm", confidence=0.9, morning=warm, afternoon=warm, evening=warm),
        ]
    )
    assert selected is not None
    assert selected.source_id == "warm"


def test_optimistic_tie_breaker_uses_confidence_then_id() -> None:
    a = _dp(rain=10, sun=2, temp=15)
    hi = make_source(source_id="z", confidence=0.95, morning=a, afternoon=a, evening=a)
    lo = make_source(source_id="m", confidence=0.5, morning=a, afternoon=a, evening=a)
    _, selected = build_optimistic_forecast([hi, lo])
    assert selected is not None
    assert selected.source_id == "z"


def test_pending_when_no_ranking_candidates() -> None:
    d = _dp()
    hidden = make_source(
        source_id="one",
        confidence=1.0,
        morning=d,
        afternoon=d,
        evening=d,
        ranking_eligible=False,
    )
    consensus, selected = build_optimistic_forecast([hidden])
    assert consensus.status == "pending"
    assert selected is None


def test_fallback_only_source_not_ranking_candidate() -> None:
    dayparts = ForecastDayparts(
        morning=_dp(),
        afternoon=_dp(),
        evening=_dp(),
    )
    only_fallback = SourceForecast(
        source_id="fb",
        source_name="fb",
        fetched_at="2026-04-09T12:00:00Z",
        target_date="2026-04-10",
        source_url="https://example.com",
        method="html",
        confidence=0.9,
        status="available",
        note=None,
        dayparts=dayparts,
        ranking_eligible=False,
    )
    assert is_ranking_candidate(only_fallback) is False


def test_ranking_candidate_requires_flag_and_complete() -> None:
    ok = make_source(
        source_id="ok",
        confidence=0.9,
        morning=_dp(),
        afternoon=_dp(),
        evening=_dp(),
    )
    assert is_ranking_candidate(ok) is True
