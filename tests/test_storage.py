import json
from datetime import date

from forecast_pipeline.models import ConsensusForecast, ForecastMetrics, SourceForecast
from forecast_pipeline.storage import write_latest


def test_write_latest(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("forecast_pipeline.storage.DATA_DIR", tmp_path)
    source = SourceForecast(
        source_id="dwd",
        source_name="DWD",
        fetched_at="2026-04-09T12:00:00Z",
        target_date="2026-05-01",
        source_url="https://example.com",
        method="html",
        confidence=0.8,
        status="available",
        note=None,
        metrics=ForecastMetrics(temp_min_c=8.0, temp_max_c=18.0, precip_probability_pct=20.0),
    )
    consensus = ConsensusForecast(
        status="available",
        label="Verdichtete Prognose",
        note="1 Quelle",
        source_count=1,
        confidence=0.8,
        spread={"temp_max_c": 0.0, "temp_min_c": 0.0, "precip_probability_pct": 0.0, "wind_kph": None},
        metrics=ForecastMetrics(temp_min_c=8.0, temp_max_c=18.0, precip_probability_pct=20.0),
    )

    target = write_latest(
        generated_at="2026-04-09T12:00:00Z",
        target_date=date(2026, 4, 30),
        sources=[source],
        consensus=consensus,
    )
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["coverage"]["available_sources"] == 1
    assert payload["best_forecast"]["temp_max_c"] == 18.0
    assert payload["target_date"] == "2026-04-30"
