"""Attach explicit cache-admission gates to the durable artifact ledger.

This is a ledger-only transformation.  It never restores, saves, deletes, or
otherwise mutates a research or ops cache.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


GATE_COLUMNS = (
    "loss_tie_gate",
    "player_team_bridge_gate",
    "source_precedence_gate",
    "cache_admission_state",
)
CLOSED_STATUSES = {
    "cache_verified",
    "no_promotable_candidate_emitted",
    "not_data_bearing",
}
RAW_ARTIFACT_RECORD_TYPE = "artifact_inventory"
CACHE_RECOVERY_RECORD_TYPE = "cache_recovery_receipt"


def _record_type(row: dict[str, str]) -> str:
    """Classify legacy numeric artifact rows separately from cache receipts."""
    recorded = str(row.get("record_type", "") or "").strip()
    if recorded:
        return recorded
    artifact_id = str(row.get("artifact_id", "") or "").strip()
    return (
        RAW_ARTIFACT_RECORD_TYPE
        if artifact_id.isdigit()
        else CACHE_RECOVERY_RECORD_TYPE
    )


def _number(row: dict[str, str], field: str) -> int:
    return int(str(row.get(field, "") or "0"))


def derive_gate_states(row: dict[str, str]) -> dict[str, str]:
    """Return the exact remaining cache-admission gates for one artifact."""
    status = str(row.get("final_status", "") or "")
    if status in CLOSED_STATUSES:
        return {
            "loss_tie_gate": "not_required",
            "player_team_bridge_gate": "not_required",
            "source_precedence_gate": "not_required",
            "cache_admission_state": f"closed_{status}",
        }

    # `blocked_schema_cells` is frozen historical evidence from artifacts that
    # predated the approved cache's loss/tie columns.  Loss and tie are now
    # part of the one canonical schema, so this count cannot remain an active
    # admission gate.  Keep the count in the raw receipt, but state plainly
    # that schema support is closed rather than forcing stale work items to
    # claim a schema migration is still required.
    historical_loss_tie = _number(row, "blocked_schema_cells") > 0
    bridge = _number(row, "unmatched_cache_cells") > 0
    precedence = _number(row, "cache_conflict_cells") > 0
    gates = {
        "loss_tie_gate": "closed_schema_supported" if historical_loss_tie else "not_required",
        "player_team_bridge_gate": (
            "open_raw_roster_bridge_required" if bridge else "not_required"
        ),
        "source_precedence_gate": (
            "open_field_precedence_required" if precedence else "not_required"
        ),
    }
    names = [
        name
        for name, required in (
            ("player_team_bridge", bridge),
            ("source_precedence", precedence),
        )
        if required
    ]
    gates["cache_admission_state"] = (
        "open_all_required_gates"
        if len(names) == 3
        else "open_" + "_and_".join(names)
        if names else f"open_unclassified_{status}"
    )
    return gates


def enrich(*, ledger_path: Path, out_path: Path) -> dict[str, int]:
    with ledger_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fields = list(reader.fieldnames or [])
    if not rows:
        raise SystemExit("ledger is empty")
    if "record_type" not in fields:
        fields.insert(0, "record_type")
    for field in GATE_COLUMNS:
        if field not in fields:
            fields.append(field)
    for row in rows:
        row["record_type"] = _record_type(row)
        row.update(derive_gate_states(row))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return {
        "ledger_rows": len(rows),
        "raw_artifact_rows": sum(
            row["record_type"] == RAW_ARTIFACT_RECORD_TYPE for row in rows
        ),
        "cache_recovery_receipt_rows": sum(
            row["record_type"] == CACHE_RECOVERY_RECORD_TYPE for row in rows
        ),
        "closed_rows": sum(
            row["record_type"] == RAW_ARTIFACT_RECORD_TYPE
            and row["cache_admission_state"].startswith("closed_")
            for row in rows
        ),
        "open_rows": sum(
            row["record_type"] == RAW_ARTIFACT_RECORD_TYPE
            and row["cache_admission_state"].startswith("open_")
            for row in rows
        ),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    print(json.dumps(enrich(ledger_path=args.ledger, out_path=args.out), sort_keys=True))


if __name__ == "__main__":
    main()
