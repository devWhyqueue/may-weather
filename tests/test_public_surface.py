"""Keep public fetch/adapter entry points referenced for static analysis."""

from __future__ import annotations

import forecast_pipeline.fetcher as fetcher_mod
from forecast_pipeline.adapters.sources import BaseSourceAdapter


def test_fetcher_exports_are_callable() -> None:
    assert callable(fetcher_mod.fetch_and_score)
    assert callable(fetcher_mod.resolve_best_target_date)


def test_base_adapter_defines_fetch() -> None:
    assert callable(BaseSourceAdapter.fetch)
