from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceDefinition:
    """Describes one external weather source and how strongly it should be weighted."""

    source_id: str
    source_name: str
    source_url: str
    method: str
    weight: float
    horizon_days: int | None = None
