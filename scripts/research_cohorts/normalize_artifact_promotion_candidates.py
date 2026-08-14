"""Normalize one mapped source row into field-level, non-mutating candidates."""
from __future__ import annotations

from typing import Any


KEY_FIELDS = ("db_name", "year", "week", "NFL_player_id", "team_key", "manager")


def normalize_source_row(
    *,
    artifact_id: str,
    source_grain: str,
    source_key: tuple[str, ...],
    source_to_target: tuple[tuple[str, str], ...],
    row: dict[str, Any],
) -> list[dict[str, Any]]:
    """Emit only source-backed candidate cells after proving its source key.

    The function intentionally does not join or write the canonical cache. A
    later guarded stage performs that operation using the grain encoded here.
    """
    for field in source_key:
        value = row.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ValueError(f"missing required source key: {field}")

    identity = {field: row.get(field) for field in KEY_FIELDS}
    candidates: list[dict[str, Any]] = []
    for source_column, target_column in source_to_target:
        value = row.get(source_column)
        if value is None:
            continue
        candidates.append({
            "artifact_id": str(artifact_id),
            "source_grain": source_grain,
            **identity,
            "source_column": source_column,
            "target_column": target_column,
            "source_value": value,
        })
    return candidates
