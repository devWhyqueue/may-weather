from __future__ import annotations

from datetime import date, timedelta

import httpx

from forecast_pipeline.adapters.sources import build_source_adapters
from forecast_pipeline.config import max_horizon_days, preferred_target_date
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


def resolve_best_target_date(*, fetched_at: str) -> date:
    """Pick the available forecast date closest to May 1, preferring an exact match when present."""

    today = date.today()
    preferred = preferred_target_date(today)
    best_candidate: date | None = None
    best_distance: tuple[int, int] | None = None
    for offset in range(max_horizon_days(), -1, -1):
        candidate = today + timedelta(days=offset)
        _, consensus = fetch_and_score(target_date=candidate, fetched_at=fetched_at)
        if consensus.source_count == 0:
            continue
        distance = (
            abs((candidate - preferred).days),
            0 if candidate <= preferred else 1,
        )
        if best_distance is None or distance < best_distance:
            best_candidate = candidate
            best_distance = distance
    if best_candidate is not None:
        return best_candidate
    return today
