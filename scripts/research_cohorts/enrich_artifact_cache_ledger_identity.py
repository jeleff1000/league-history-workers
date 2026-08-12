"""Attach existing raw-signal identity receipts to the artifact cache ledger.

This utility only enriches the CSV evidence ledger.  It never opens, writes,
renames, saves, or replaces a research cache.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path


IDENTITY_COLUMNS = [
    "identity_profile_run_id",
    "identity_profile_source",
    "source_team_keys",
    "matched_source_team_keys",
    "unmatched_source_team_keys",
    "league_week_overlap_keys",
    "league_week_absent_keys",
    "matched_source_player_rows",
    "matched_by_mfl_manager_guid",
    "matched_by_manager_team_name",
    "matched_by_unique_manager",
    "cache_join_strategy",
    "identity_bridge_status",
]


def _run_id(path: Path) -> int:
    matches = re.findall(r"(?<!\d)(\d{8,})(?!\d)", str(path))
    return max((int(value) for value in matches), default=0)


def _rows(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        rows = payload.get("rows", [])
    else:
        rows = payload
    return [row for row in rows if isinstance(row, dict) and row.get("artifact_id") is not None]


def _bridge_status(row: dict) -> str:
    if "source_team_keys" not in row:
        return "player_key_profiled"
    source = int(row.get("source_team_keys", 0) or 0)
    matched = int(row.get("matched_source_team_keys", 0) or 0)
    unmatched = int(row.get("unmatched_source_team_keys", 0) or 0)
    absent = int(row.get("league_week_absent_keys", 0) or 0)
    if source and unmatched == 0:
        return "fully_matched"
    if matched == 0 and absent:
        return "source_only_or_no_player_rows"
    if matched == 0:
        return "no_player_team_bridge"
    if absent:
        return "partial_bridge_with_source_only"
    return "partial_bridge"


def _join_strategy(row: dict) -> str:
    if "source_team_keys" not in row:
        return "strict_player_key"
    strategies = {
        "mfl_manager_guid_team_fanout": int(row.get("matched_by_mfl_manager_guid", 0) or 0),
        "manager_team_name_fanout": int(row.get("matched_by_manager_team_name", 0) or 0),
        "manager_week_fanout": int(row.get("matched_by_unique_manager", 0) or 0),
    }
    resolved = [name for name, count in strategies.items() if count]
    if not resolved:
        return "unresolved_team_identity"
    return resolved[0] if len(resolved) == 1 else "mixed_resolved_team_identity"


def enrich(*, ledger_path: Path, profile_paths: list[Path], out_path: Path) -> dict[str, int]:
    ledger_rows = list(csv.DictReader(ledger_path.open(newline="", encoding="utf-8")))
    if not ledger_rows:
        raise SystemExit("ledger is empty")
    raw_rows = [
        row for row in ledger_rows
        if row.get("record_type") == "artifact_inventory"
    ]
    if any(
        row.get("record_type") not in {"artifact_inventory", "cache_recovery_receipt"}
        for row in ledger_rows
    ):
        raise SystemExit("ledger has an unknown record type")
    ids = {int(row["artifact_id"]) for row in raw_rows}
    if len(ids) != len(raw_rows):
        raise SystemExit("ledger has duplicate artifact IDs")

    latest: dict[int, tuple[int, Path, dict]] = {}
    unknown: set[int] = set()
    for path in profile_paths:
        if not path.exists():
            raise SystemExit(f"profile missing: {path}")
        run_id = _run_id(path)
        for row in _rows(path):
            artifact_id = int(row["artifact_id"])
            if artifact_id not in ids:
                unknown.add(artifact_id)
                continue
            prior = latest.get(artifact_id)
            if prior is None or run_id >= prior[0]:
                latest[artifact_id] = (run_id, path, row)

    fields = list(ledger_rows[0].keys())
    for column in IDENTITY_COLUMNS:
        if column not in fields:
            fields.append(column)
    for ledger_row in ledger_rows:
        if ledger_row.get("record_type") != "artifact_inventory":
            continue
        profile = latest.get(int(ledger_row["artifact_id"]))
        for column in IDENTITY_COLUMNS:
            ledger_row[column] = ""
        if profile is None:
            continue
        run_id, path, evidence = profile
        ledger_row["identity_profile_run_id"] = str(run_id) if run_id else ""
        ledger_row["identity_profile_source"] = path.name
        for column in (
            "source_team_keys", "matched_source_team_keys", "unmatched_source_team_keys",
            "league_week_overlap_keys", "league_week_absent_keys", "matched_source_player_rows",
            "matched_by_mfl_manager_guid", "matched_by_manager_team_name", "matched_by_unique_manager",
        ):
            value = evidence.get(column)
            ledger_row[column] = "" if value is None else str(int(value))
        ledger_row["cache_join_strategy"] = _join_strategy(evidence)
        ledger_row["identity_bridge_status"] = _bridge_status(evidence)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(ledger_rows)
    return {
        "ledger_rows": len(ledger_rows),
        "profiled_artifacts": len(latest),
        "unmatched_profile_artifacts": len(unknown),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--profile", type=Path, action="append", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    print(json.dumps(enrich(ledger_path=args.ledger, profile_paths=args.profile, out_path=args.out), sort_keys=True))


if __name__ == "__main__":
    main()
