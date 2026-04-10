from datetime import date

from forecast_pipeline.config import common_horizon_days, pipeline_target_date, preferred_target_date


def test_pipeline_target_caps_at_common_horizon_before_may1() -> None:
    today = date(2026, 4, 10)
    assert preferred_target_date(today) == date(2026, 5, 1)
    assert common_horizon_days() == 14
    assert pipeline_target_date(today) == date(2026, 4, 24)


def test_pipeline_target_uses_may1_when_inside_horizon() -> None:
    today = date(2026, 4, 20)
    assert pipeline_target_date(today) == date(2026, 5, 1)
