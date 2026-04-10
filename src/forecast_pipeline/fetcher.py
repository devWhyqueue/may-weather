from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date

import httpx

from forecast_pipeline.adapters.html_payloads import PagePayload
from forecast_pipeline.sources import BaseSourceAdapter, build_source_adapters
from forecast_pipeline.config import pipeline_target_date, preferred_target_date
from forecast_pipeline.models import (
    ConsensusForecast,
    SelectedDisplaySource,
    SourceForecast,
    now_utc_iso,
)
from forecast_pipeline.scoring import build_optimistic_forecast
from forecast_pipeline.storage import update_history, write_latest, write_meta

logger = logging.getLogger(__name__)


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
    """Fetch every configured source page and return payloads or transport errors."""

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
                loaded.append(
                    LoadedSourcePage(adapter=adapter, page=adapter.load_page(client))
                )
            except Exception as exc:
                loaded.append(LoadedSourcePage(adapter=adapter, error=exc))
    return loaded


def source_results_for_target(
    loaded_pages: list[LoadedSourcePage],
    *,
    target_date: date,
    fetched_at: str,
) -> list[SourceForecast]:
    """Run each adapter parser for a single calendar target date."""

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
    """Load pages and parse them into `SourceForecast` rows for one date."""

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
) -> tuple[list[SourceForecast], ConsensusForecast, SelectedDisplaySource | None]:
    """Fetch all sources and compute the optimistic single-provider display forecast."""

    sources = fetch_all_sources(
        target_date=target_date,
        fetched_at=fetched_at,
        source_filter=source_filter,
        headed=headed,
    )
    consensus, selected = build_optimistic_forecast(sources)
    return sources, consensus, selected


def resolve_best_target_date(
    *,
    fetched_at: str,
    headed: bool = False,
    source_filter: str | None = None,
) -> date:
    """Return the shared pipeline target date (May 1 capped by common provider horizon)."""

    del headed, source_filter
    return resolve_best_target_date_from_pages([], fetched_at=fetched_at)


def resolve_best_target_date_from_pages(
    loaded_pages: list[LoadedSourcePage],
    *,
    fetched_at: str,
) -> date:
    """Same target as `resolve_best_target_date`; pages are ignored (kept for call-site reuse)."""

    del loaded_pages, fetched_at
    return pipeline_target_date()


def _fetch_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch forecast data for Haltern am See."
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Open browser-backed sources in headed mode for debugging.",
    )
    parser.add_argument("--source", help="Restrict fetches to one source ID.")
    return parser


def _run_fetch(source_filter: str | None, *, headed: bool) -> tuple[str, str]:
    generated_at = now_utc_iso()
    loaded_pages = load_source_pages(source_filter=source_filter, headed=headed)
    target_date = pipeline_target_date()
    sources = source_results_for_target(
        loaded_pages,
        target_date=target_date,
        fetched_at=generated_at,
    )
    consensus, selected = build_optimistic_forecast(sources)
    latest_path = write_latest(
        generated_at=generated_at,
        target_date=target_date,
        sources=sources,
        consensus=consensus,
        selected=selected,
    )
    meta_path = write_meta(
        generated_at=generated_at,
        target_date=target_date,
        sources=sources,
        consensus=consensus,
        selected=selected,
    )
    return str(latest_path), str(meta_path)


def main_fetch() -> None:
    """Fetch source data and regenerate the latest static JSON artifacts."""

    args = _fetch_parser().parse_args()
    latest_path, meta_path = _run_fetch(args.source, headed=args.headed)
    logger.info(f"Wrote {latest_path}")
    logger.info(f"Wrote {meta_path}")


def main_build_history() -> None:
    """Append the current latest snapshot to the history JSON file."""

    parser = argparse.ArgumentParser(
        description="Update history.json from latest.json."
    )
    parser.parse_args()
    history_path = update_history(generated_at=now_utc_iso())
    logger.info(f"Wrote {history_path}")


_CLI_ENTRYPOINTS = {
    "weather-fetch": main_fetch,
    "weather-build-history": main_build_history,
}
_FETCHER_PUBLIC_API = (
    fetch_and_score,
    resolve_best_target_date,
    preferred_target_date,
    pipeline_target_date,
)
