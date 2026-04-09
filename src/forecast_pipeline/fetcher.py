from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import httpx

from forecast_pipeline.adapters.sources import BaseSourceAdapter, PagePayload, build_source_adapters
from forecast_pipeline.config import max_horizon_days
from forecast_pipeline.models import ConsensusForecast, SourceForecast
from forecast_pipeline.scoring import build_consensus


@dataclass(frozen=True)
class LoadedSourcePage:
    adapter: BaseSourceAdapter
    page: PagePayload | None = None
    error: Exception | None = None


def _build_client() -> httpx.Client:
    return httpx.Client(
        timeout=15.0,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0 Safari/537.36",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        },
    )


def load_source_pages(
    *,
    source_filter: str | None = None,
    headed: bool = False,
) -> list[LoadedSourcePage]:
    adapters = build_source_adapters(headed=headed)
    if source_filter:
        adapters = [
            adapter
            for adapter in adapters
            if adapter.definition.source_id == source_filter
        ]

    loaded: list[LoadedSourcePage] = []
    with _build_client() as client:
        for adapter in adapters:
            try:
                loaded.append(LoadedSourcePage(adapter=adapter, page=adapter.load_page(client)))
            except Exception as exc:
                loaded.append(LoadedSourcePage(adapter=adapter, error=exc))
    return loaded


def source_results_for_target(
    loaded_pages: list[LoadedSourcePage],
    *,
    target_date: date,
    fetched_at: str,
) -> list[SourceForecast]:
    results: list[SourceForecast] = []
    for loaded in loaded_pages:
        if loaded.page is None:
            assert loaded.error is not None
            results.append(
                loaded.adapter.error_result(
                    fetched_at=fetched_at,
                    target_date=target_date,
                    exc=loaded.error,
                )
            )
            continue
        results.append(
            loaded.adapter.page_to_result(
                loaded.page,
                fetched_at=fetched_at,
                target_date=target_date,
            )
        )
    return results


def fetch_all_sources(
    *,
    target_date: date,
    fetched_at: str,
    source_filter: str | None = None,
    headed: bool = False,
) -> list[SourceForecast]:
    loaded_pages = load_source_pages(source_filter=source_filter, headed=headed)
    return source_results_for_target(
        loaded_pages,
        target_date=target_date,
        fetched_at=fetched_at,
    )


def fetch_and_score(
    *,
    target_date: date,
    fetched_at: str,
    source_filter: str | None = None,
    headed: bool = False,
) -> tuple[list[SourceForecast], ConsensusForecast]:
    sources = fetch_all_sources(
        target_date=target_date,
        fetched_at=fetched_at,
        source_filter=source_filter,
        headed=headed,
    )
    return sources, build_consensus(sources)


def resolve_best_target_date(
    *,
    fetched_at: str,
    headed: bool = False,
    source_filter: str | None = None,
) -> date:
    loaded_pages = load_source_pages(source_filter=source_filter, headed=headed)
    return resolve_best_target_date_from_pages(loaded_pages, fetched_at=fetched_at)


def resolve_best_target_date_from_pages(
    loaded_pages: list[LoadedSourcePage],
    *,
    fetched_at: str,
) -> date:
    today = date.today()
    best_candidate: date | None = None
    best_score: tuple[int, int] | None = None

    for offset in range(0, min(2, max_horizon_days()) + 1):
        candidate = today + timedelta(days=offset)
        sources = source_results_for_target(
            loaded_pages,
            target_date=candidate,
            fetched_at=fetched_at,
        )
        available = len([source for source in sources if source.status == "available"])
        score = (available, offset)
        if best_score is None or score > best_score:
            best_candidate = candidate
            best_score = score

    return best_candidate or today
