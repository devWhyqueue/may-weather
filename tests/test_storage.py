import json
from datetime import date

from forecast_pipeline.models import (
    ConsensusForecast,
    DaypartForecast,
    ForecastDayparts,
    SourceForecast,
)
from forecast_pipeline.storage import write_latest


def test_write_latest(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("forecast_pipeline.storage.DATA_DIR", tmp_path)
    dayparts = ForecastDayparts(
        morning=DaypartForecast(condition_summary="Bewölkt", precip_probability_pct=20.0, sunshine_hours=2.0),
        afternoon=DaypartForecast(condition_summary="Wolkig", precip_probability_pct=10.0, sunshine_hours=3.0),
        evening=DaypartForecast(condition_summary="Sonnig", precip_probability_pct=5.0, sunshine_hours=1.0),
    )
    source = SourceForecast(
        source_id="demo",
        source_name="Demo",
        fetched_at="2026-04-09T12:00:00Z",
        target_date="2026-04-10",
        source_url="https://example.com",
        method="html",
        confidence=0.8,
        status="available",
        note=None,
        dayparts=dayparts,
    )
    consensus = ConsensusForecast(
        status="available",
        label="Verdichtete Tagesabschnitte",
        note="1 Quelle",
        source_count=1,
        confidence=0.8,
        spread={
            "morning": {"precip_probability_pct": 0.0, "sunshine_hours": 0.0},
            "afternoon": {"precip_probability_pct": 0.0, "sunshine_hours": 0.0},
            "evening": {"precip_probability_pct": 0.0, "sunshine_hours": 0.0},
        },
        dayparts=dayparts,
    )

    target = write_latest(
        generated_at="2026-04-09T12:00:00Z",
        target_date=date(2026, 4, 10),
        sources=[source],
        consensus=consensus,
    )
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["coverage"]["available_sources"] == 1
    assert payload["best_forecast"]["dayparts"]["morning"]["sunshine_hours"] == 2.0
    assert payload["target_date"] == "2026-04-10"
