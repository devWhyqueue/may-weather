import argparse
import logging

from forecast_pipeline.fetcher import (
    fetch_and_score,
    load_source_pages,
    resolve_best_target_date,
    resolve_best_target_date_from_pages,
    source_results_for_target,
)
from forecast_pipeline.config import preferred_target_date
from forecast_pipeline.models import now_utc_iso
from forecast_pipeline.scoring import build_optimistic_forecast
from forecast_pipeline.storage import update_history, write_latest, write_meta

logger = logging.getLogger(__name__)


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
    target_date = resolve_best_target_date_from_pages(
        loaded_pages,
        fetched_at=generated_at,
    )
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

# Referenced for library-style use and static dead-code analysis.
_FETCHER_PUBLIC_API = (fetch_and_score, resolve_best_target_date)
_CONFIG_PUBLIC_API = (preferred_target_date,)
