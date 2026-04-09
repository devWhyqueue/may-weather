from forecast_pipeline.models import DaypartForecast, ForecastDayparts, SourceForecast
from forecast_pipeline.scoring import build_consensus


def make_source(*, source_id: str, confidence: float, rain: float, sun: float, status: str = "available") -> SourceForecast:
    dayparts = ForecastDayparts(
        morning=DaypartForecast(condition_summary="Bewölkt", precip_probability_pct=rain, sunshine_hours=sun),
        afternoon=DaypartForecast(condition_summary="Wolkig", precip_probability_pct=rain / 2, sunshine_hours=sun + 1),
        evening=DaypartForecast(condition_summary="Sonnig", precip_probability_pct=rain / 4, sunshine_hours=sun / 2),
    )
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
    )


def test_build_consensus_weighted_mean() -> None:
    consensus = build_consensus(
        [
            make_source(source_id="one", confidence=1.0, rain=20.0, sun=2.0),
            make_source(source_id="two", confidence=0.5, rain=40.0, sun=4.0),
        ]
    )
    assert consensus.status == "available"
    assert consensus.dayparts.morning.precip_probability_pct == 26.7
    assert consensus.dayparts.morning.sunshine_hours == 2.7
    assert consensus.source_count == 2


def test_build_consensus_pending_when_no_complete_sources() -> None:
    incomplete = make_source(source_id="one", confidence=1.0, rain=20.0, sun=2.0, status="partial")
    consensus = build_consensus([incomplete])
    assert consensus.status == "pending"
    assert consensus.source_count == 0
