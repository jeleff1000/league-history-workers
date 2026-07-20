"""Redacted pilot plan construction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from scripts.yahoo_corpus.scheduler import Candidate, select_cross_era, select_spacing


@dataclass(frozen=True)
class Plan:
    mode: str
    requested: int
    tasks: tuple[Candidate, ...]

    @property
    def shortfall(self) -> int:
        return max(0, self.requested - len(self.tasks))

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "requested": self.requested,
            "shortfall": self.shortfall,
            "tasks": [
                {
                    "task_id": row.task_id,
                    "grant_id": row.grant_id,
                    "league_key": row.league_key,
                    "season": row.season,
                    "cohort_slug": row.cohort_slug,
                    "lineage_id": row.lineage_id,
                    "identity_id": row.identity_id,
                    "era": row.era,
                }
                for row in self.tasks
            ],
        }


def build_plan(mode: str, candidates: Iterable[Candidate], *, limit: int = 12) -> Plan:
    rows = list(candidates)
    if mode == "cross-era":
        selected = select_cross_era(rows, limit=limit)
    elif mode == "spacing-resume":
        years_per_grant = 3
        grants = max(1, limit // years_per_grant)
        selected = select_spacing(rows, grants=grants, years_per_grant=years_per_grant)[:limit]
    elif mode == "inventory":
        # The full corpus: every discovered league-year, deduped by task.
        # The credential scheduler's grant-LRU rotation provides maximal
        # spacing between any one grant's imports; limit <= 0 means all.
        seen: set[str] = set()
        selected = []
        for row in rows:
            if row.task_id in seen:
                continue
            seen.add(row.task_id)
            selected.append(row)
        if limit > 0:
            selected = selected[:limit]
    else:
        raise ValueError(f"Unknown Yahoo corpus plan mode: {mode}")
    requested = limit if limit > 0 else len(selected)
    return Plan(mode=mode, requested=requested, tasks=tuple(selected))
