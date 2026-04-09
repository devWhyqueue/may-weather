from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from forecast_pipeline.config import DATA_DIR, LOCATION
from forecast_pipeline.models import (
    ConsensusForecast,
    SelectedDisplaySource,
    SourceForecast,
)
from forecast_pipeline.scoring import DAYPARTS, is_ranking_candidate


def _has_signal(source: SourceForecast) -> bool:
    return all(
        getattr(source.dayparts, daypart).condition_summary is not None
        and getattr(source.dayparts, daypart).precip_probability_pct is not None
        and getattr(source.dayparts, daypart).sunshine_hours is not None
        and getattr(source.dayparts, daypart).temperature_celsius is not None
        for daypart in DAYPARTS
    )


def ensure_data_dir() -> None:
    """Create the static `site/data` directory when missing."""

    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _location_payload() -> dict[str, Any]:
    return {
        "name": LOCATION.name,
        "country": LOCATION.country,
        "region": LOCATION.region,
        "latitude": LOCATION.latitude,
        "longitude": LOCATION.longitude,
        "timezone": LOCATION.timezone,
    }


def _available_count(sources: list[SourceForecast]) -> int:
    return len(
        [
            source
            for source in sources
            if source.status == "available" and _has_signal(source)
        ]
    )


def _latest_payload(
    *,
    generated_at: str,
    target_date: date,
    sources: list[SourceForecast],
    consensus: ConsensusForecast,
    selected: SelectedDisplaySource | None,
) -> dict[str, Any]:
    return {
        "location": _location_payload(),
        "target_date": target_date.isoformat(),
        "generated_at": generated_at,
        "best_forecast": consensus.to_dict(),
        "selected_source": None
        if selected is None
        else {
            "source_id": selected.source_id,
            "source_name": selected.source_name,
            "source_url": selected.source_url,
        },
        "coverage": {
            "total_sources": len(sources),
            "available_sources": _available_count(sources),
            "ranking_candidates": len([s for s in sources if is_ranking_candidate(s)]),
        },
        "confidence": consensus.confidence,
        "sources": [source.to_dict() for source in sources],
    }


def write_latest(
    *,
    generated_at: str,
    target_date: date,
    sources: list[SourceForecast],
    consensus: ConsensusForecast,
    selected: SelectedDisplaySource | None = None,
) -> Path:
    """Persist the main microsite payload to `latest.json`."""

    ensure_data_dir()
    latest_path = DATA_DIR / "latest.json"
    _dump_json(
        latest_path,
        _latest_payload(
            generated_at=generated_at,
            target_date=target_date,
            sources=sources,
            consensus=consensus,
            selected=selected,
        ),
    )
    return latest_path


def write_meta(
    *,
    generated_at: str,
    target_date: date,
    sources: list[SourceForecast],
    consensus: ConsensusForecast,
    selected: SelectedDisplaySource | None = None,
) -> Path:
    """Write compact run metadata to `meta.json`."""

    ensure_data_dir()
    meta_path = DATA_DIR / "meta.json"
    payload = {
        "generated_at": generated_at,
        "target_date": target_date.isoformat(),
        "location": LOCATION.name,
        "source_status": {
            "available": len(
                [source for source in sources if source.status == "available"]
            ),
            "partial": len(
                [source for source in sources if source.status == "partial"]
            ),
            "unavailable": len(
                [source for source in sources if source.status == "unavailable"]
            ),
            "error": len([source for source in sources if source.status == "error"]),
        },
        "best_forecast_status": consensus.status,
        "selected_source_id": None if selected is None else selected.source_id,
    }
    _dump_json(meta_path, payload)
    return meta_path


def read_latest() -> dict[str, Any]:
    """Load and parse `latest.json` from disk."""

    latest_path = DATA_DIR / "latest.json"
    return json.loads(latest_path.read_text(encoding="utf-8"))


def _load_history(history_path: Path) -> dict[str, Any]:
    if history_path.exists():
        return json.loads(history_path.read_text(encoding="utf-8"))
    latest = read_latest()
    return {"target_date": latest["target_date"], "snapshots": []}


def _snapshot_from_latest(latest: dict[str, Any], generated_at: str) -> dict[str, Any]:
    return {
        "fetched_at": generated_at,
        "best_forecast": latest["best_forecast"],
        "source_count": latest["coverage"]["available_sources"],
        "selected_source_id": (latest.get("selected_source") or {}).get("source_id"),
    }


def update_history(*, generated_at: str) -> Path:
    """Append the current `latest.json` snapshot to `history.json`."""

    ensure_data_dir()
    latest = read_latest()
    history_path = DATA_DIR / "history.json"
    history = _load_history(history_path)
    if history["target_date"] != latest["target_date"]:
        history = {"target_date": latest["target_date"], "snapshots": []}
    snapshots = [
        item for item in history["snapshots"] if item["fetched_at"] != generated_at
    ]
    snapshots.append(_snapshot_from_latest(latest, generated_at))
    snapshots.sort(key=lambda item: item["fetched_at"])
    history["snapshots"] = snapshots
    _dump_json(history_path, history)
    return history_path
