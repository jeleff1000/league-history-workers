"""Fail-closed state helpers for the ordered MFL snapshot publisher.

This module deliberately has no GitHub, cache, or database side effects.  It
turns the immutable quota manifest into a deterministic reservation window so
the workflow can persist the returned state as an artifact before dispatching
fetch workers.
"""
from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any


REQUIRED_MANIFEST_FIELDS = {
    "stage",
    "target_per_year",
    "year_order",
    "year_ordinal",
    "season",
    "league_id",
    "source",
}
_PLAIN_ID = re.compile(r"\d{1,5}")
_DB_NAME = re.compile(r"mfl_(\d{4})_(\d{5})")
TERMINAL_FETCH_STATUSES = {
    "accepted",
    "fetch_failed",
    "validation_failed",
    "malformed",
    "skipped",
    "unavailable_source",
}
RETRY_STATUSES = TERMINAL_FETCH_STATUSES - {"accepted", "skipped"}


class PublisherStateError(ValueError):
    """The requested reservation would violate the immutable manifest contract."""


def require_current_parent(*, expected_parent_snapshot_id: str, current_snapshot_id: str) -> None:
    """Enforce compare-and-swap semantics before a publisher may write."""
    if not expected_parent_snapshot_id or not current_snapshot_id:
        raise PublisherStateError("publisher parent snapshot ID is required")
    if expected_parent_snapshot_id != current_snapshot_id:
        raise PublisherStateError(
            "stale publisher: expected parent "
            f"{expected_parent_snapshot_id!r}, current snapshot is {current_snapshot_id!r}"
        )


def normalize_mfl_league_id(season: int, value: object) -> str:
    """Return the canonical five-digit MFL ID or reject unsupported input."""
    raw = str(value).strip()
    if _PLAIN_ID.fullmatch(raw):
        return raw.zfill(5)
    match = _DB_NAME.fullmatch(raw)
    if match and int(match.group(1)) == int(season):
        return match.group(2)
    raise PublisherStateError(f"malformed MFL league_id {value!r} for season {season}")


def _validate_manifest(manifest: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = [dict(row) for row in manifest]
    if not rows:
        raise PublisherStateError("manifest is empty")
    identities: set[tuple[int, str]] = set()
    ordinals: dict[tuple[int, int], int] = defaultdict(int)
    for position, row in enumerate(rows):
        missing = REQUIRED_MANIFEST_FIELDS - row.keys()
        if missing:
            raise PublisherStateError(f"manifest position {position} missing fields: {sorted(missing)}")
        try:
            stage = int(row["stage"])
            season = int(row["season"])
            year_order = int(row["year_order"])
            ordinal = int(row["year_ordinal"])
        except (TypeError, ValueError) as exc:
            raise PublisherStateError(f"manifest position {position} has non-integer ordering fields") from exc
        if stage < 1 or season < 1900 or year_order != season:
            raise PublisherStateError(f"manifest position {position} has invalid stage/year ordering")
        group = (stage, season)
        expected_ordinal = ordinals[group] + 1
        if ordinal != expected_ordinal:
            raise PublisherStateError(
                f"manifest position {position} has year_ordinal={ordinal}; expected {expected_ordinal} for {group}"
            )
        ordinals[group] = ordinal
        league_id = normalize_mfl_league_id(season, row["league_id"])
        identity = (season, league_id)
        if identity in identities:
            raise PublisherStateError(f"duplicate canonical identity in manifest: {identity}")
        identities.add(identity)
        row["stage"] = stage
        row["season"] = season
        row["year_order"] = year_order
        row["year_ordinal"] = ordinal
        row["league_id"] = league_id
    return rows


def reserve_next_rows(
    manifest: Iterable[Mapping[str, Any]],
    *,
    existing_identities: Iterable[tuple[int, object]],
    reserved_positions: set[int],
    limit: int,
) -> dict[str, object]:
    """Return the first contiguous unreserved manifest window.

    Existing identities become terminal ``already_present`` ledger rows before
    any fetch is scheduled.  A gap in already-reserved positions is refused:
    only the reservation planner may allocate parallel windows.
    """
    if limit <= 0:
        raise PublisherStateError("limit must be positive")
    rows = _validate_manifest(manifest)
    existing = {
        (int(season), normalize_mfl_league_id(int(season), league_id))
        for season, league_id in existing_identities
    }
    invalid_positions = {position for position in reserved_positions if position < 0 or position >= len(rows)}
    if invalid_positions:
        raise PublisherStateError(f"reserved positions outside manifest: {sorted(invalid_positions)}")
    start = 0
    while start in reserved_positions:
        start += 1
    if any(position > start for position in reserved_positions):
        raise PublisherStateError("non-contiguous prior reservation would break manifest order")
    selected = rows[start:start + limit]
    entries = []
    for offset, row in enumerate(selected):
        position = start + offset
        identity = (row["season"], row["league_id"])
        entries.append({
            "manifest_position": position,
            "season": row["season"],
            "league_id": row["league_id"],
            "status": "already_present" if identity in existing else "reserved",
        })
    return {"reserved": entries, "next_position": start + len(entries)}


def finalize_batch_statuses(
    reserved: Iterable[Mapping[str, Any]], *, outcomes: Iterable[Mapping[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    """Require exactly one terminal disposition for each fetchable reservation."""
    reservations = [dict(row) for row in reserved]
    by_position: dict[int, dict[str, Any]] = {}
    for row in reservations:
        try:
            position = int(row["manifest_position"])
        except (KeyError, TypeError, ValueError) as exc:
            raise PublisherStateError("reservation lacks a valid manifest_position") from exc
        if position in by_position:
            raise PublisherStateError(f"duplicate reservation at manifest position {position}")
        if row.get("status") not in {"reserved", "already_present"}:
            raise PublisherStateError(f"invalid reservation status at position {position}: {row.get('status')!r}")
        by_position[position] = row

    by_outcome: dict[int, dict[str, Any]] = {}
    for outcome in outcomes:
        row = dict(outcome)
        try:
            position = int(row["manifest_position"])
        except (KeyError, TypeError, ValueError) as exc:
            raise PublisherStateError("outcome lacks a valid manifest_position") from exc
        if position not in by_position or by_position[position]["status"] != "reserved":
            raise PublisherStateError(f"outcome does not correspond to a fetchable reservation: {position}")
        if position in by_outcome:
            raise PublisherStateError(f"duplicate outcome at manifest position {position}")
        if row.get("status") not in TERMINAL_FETCH_STATUSES:
            raise PublisherStateError(f"invalid terminal outcome at position {position}: {row.get('status')!r}")
        if row["status"] == "accepted" and not str(row.get("artifact_id", "")).strip():
            raise PublisherStateError(f"accepted outcome at position {position} lacks an immutable artifact_id")
        by_outcome[position] = row

    rows: list[dict[str, Any]] = []
    retries: list[dict[str, Any]] = []
    for position in sorted(by_position):
        reservation = by_position[position]
        if reservation["status"] == "already_present":
            if position in by_outcome:
                raise PublisherStateError(f"already-present position {position} may not be fetched")
            rows.append(dict(reservation))
            continue
        outcome = by_outcome.get(position)
        if outcome is None:
            raise PublisherStateError(f"reserved position {position} has no terminal outcome")
        entry = {**reservation, **outcome}
        rows.append(entry)
        if entry["status"] in RETRY_STATUSES:
            retries.append({
                "manifest_position": position,
                "season": entry["season"],
                "league_id": entry["league_id"],
                "status": entry["status"],
                "reason": str(entry.get("reason", "")),
            })
    return {"rows": rows, "retry_manifest": retries}
