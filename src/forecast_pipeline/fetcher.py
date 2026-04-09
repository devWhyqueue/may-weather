from __future__ import annotations

from datetime import date

import httpx

from forecast_pipeline.adapters.sources import build_source_adapters
from forecast_pipeline.models import ConsensusForecast, SourceForecast
from forecast_pipeline.scoring import build_consensus


def fetch_all_sources(
    *, target_date: date, fetched_at: str, source_filter: str | None = None
) -> list[SourceForecast]:
    """Fetch normalized records from all configured source adapters."""

    adapters = build_source_adapters()
    if source_filter:
        adapters = [
            adapter
            for adapter in adapters
            if adapter.definition.source_id == source_filter
        ]
    with httpx.Client(
        timeout=15.0,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0 Safari/537.36",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        },
    ) as client:
        return [
            adapter.fetch(client, target_date=target_date, fetched_at=fetched_at)
            for adapter in adapters
        ]


def fetch_and_score(
    *, target_date: date, fetched_at: str, source_filter: str | None = None
) -> tuple[list[SourceForecast], ConsensusForecast]:
    """Fetch all matching sources and build the blended consensus payload."""

    sources = fetch_all_sources(
        target_date=target_date, fetched_at=fetched_at, source_filter=source_filter
    )
    return sources, build_consensus(sources)
