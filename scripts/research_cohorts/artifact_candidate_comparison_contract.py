"""Admission rules that apply before candidate cells can touch the cache."""
from __future__ import annotations

from collections import defaultdict
from typing import Any


def _group_key(cell: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(cell.get(field) for field in (
        "artifact_id", "artifact_file", "file_instance", "source_grain",
        "db_name", "year", "week", "NFL_player_id", "team_key", "manager",
    ))


def _is_true(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes"}


def admissible_cells(cells: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Separate cache-eligible cells from semantically invalid source claims.

    The cache's ``champion`` statistic means a player started a championship
    game.  A team-season/title signal cannot grant that player-level credit.
    Even a direct player source needs a matching started source cell.
    """
    starts = defaultdict(bool)
    for cell in cells:
        if cell.get("target_column") == "is_started" and _is_true(cell.get("source_value_text")):
            starts[_group_key(cell)] = True

    admitted: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for cell in cells:
        if cell.get("target_column") != "champion":
            admitted.append(cell)
            continue
        if cell.get("source_grain") != "player_week":
            blocked.append({"cell": cell, "reason": "team_signal_cannot_award_player_championship_credit"})
        elif not starts[_group_key(cell)]:
            blocked.append({"cell": cell, "reason": "championship_candidate_missing_direct_started_evidence"})
        else:
            admitted.append(cell)
    return admitted, blocked
