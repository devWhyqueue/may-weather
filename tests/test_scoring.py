from forecast_pipeline.models import ForecastMetrics, SourceForecast
from forecast_pipeline.scoring import build_consensus


def make_source(
    *,
    source_id: str,
    confidence: float,
    temp_min_c: float | None,
    temp_max_c: float | None,
    precip_probability_pct: float | None,
    status: str = "available",
) -> SourceForecast:
    return SourceForecast(
        source_id=source_id,
        source_name=source_id,
        fetched_at="2026-04-09T12:00:00Z",
        target_date="2026-05-01",
        source_url="https://example.com",
        method="html",
        confidence=confidence,
        status=status,
        note=None,
        metrics=ForecastMetrics(
            temp_min_c=temp_min_c,
            temp_max_c=temp_max_c,
            precip_probability_pct=precip_probability_pct,
            precip_mm=None,
            wind_kph=None,
            condition_summary="Heiter",
        ),
    )


def test_build_consensus_weighted_mean() -> None:
    consensus = build_consensus(
        [
            make_source(source_id="one", confidence=1.0, temp_min_c=9.0, temp_max_c=17.0, precip_probability_pct=20.0),
            make_source(source_id="two", confidence=0.5, temp_min_c=11.0, temp_max_c=21.0, precip_probability_pct=40.0),
        ]
    )
    assert consensus.status == "available"
    assert consensus.metrics.temp_max_c == 18.3
    assert consensus.metrics.temp_min_c == 9.7
    assert consensus.metrics.precip_probability_pct == 26.7
    assert consensus.source_count == 2


def test_build_consensus_pending_when_no_sources_available() -> None:
    consensus = build_consensus(
        [
            make_source(
                source_id="blocked",
                confidence=0.0,
                temp_min_c=None,
                temp_max_c=None,
                precip_probability_pct=None,
                status="unavailable",
            )
        ]
    )
    assert consensus.status == "pending"
    assert consensus.source_count == 0

