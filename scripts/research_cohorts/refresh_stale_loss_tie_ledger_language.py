"""Refresh current ledger language after loss/tie became canonical schema fields.

This is a ledger-only migration.  It preserves the frozen raw artifact status,
counts, receipts, and artifact identity.  It changes only stale *current*
reason/action language that instructed a schema migration which has already
happened, then recomputes the machine gates from the same quantified evidence.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from enrich_artifact_cache_ledger_completion_gates import derive_gate_states


STALE_STATUSES = {
    "partial_schema_blocked_unmatched",
    "partial_schema_blocked_conflicts",
    "partial_schema_blocked_direct_identity",
}


def _current_reason(row: dict[str, str]) -> str:
    status = row["final_status"]
    conflicts = int(row.get("cache_conflict_cells", "0") or 0)
    if status == "partial_schema_blocked_unmatched":
        return (
            "Current canonical schema supports loss/tie. The quantified remaining "
            "gap is an unresolved player/team identity bridge; source values may not "
            "be upserted until an exact existing-player recipient is proven."
        )
    if status == "partial_schema_blocked_conflicts":
        return (
            "Current canonical schema supports loss/tie. The quantified remaining "
            "gap includes non-null source/cache conflicts and unresolved player/team "
            "identity; source precedence and an exact recipient are both required."
        )
    if conflicts:
        return (
            "Current canonical schema supports loss/tie. The direct source identity "
            "still requires exact player/team proof and source-precedence adjudication "
            "before a cache update."
        )
    return (
        "Current canonical schema supports loss/tie. The direct source identity still "
        "requires exact player/team proof before a cache update."
    )


def _next_action(action: str) -> str:
    prefix = "add_loss_tie_schema_then_"
    return action[len(prefix):] if action.startswith(prefix) else action


def refresh(*, ledger_path: Path, out_path: Path) -> dict[str, int]:
    with ledger_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fields = list(reader.fieldnames or [])
    changed = 0
    for row in rows:
        if row.get("record_type") != "artifact_inventory":
            continue
        if row.get("final_status") not in STALE_STATUSES:
            continue
        row["final_reason"] = _current_reason(row)
        row["next_action"] = _next_action(str(row.get("next_action", "") or ""))
        row.update(derive_gate_states(row))
        changed += 1
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return {"ledger_rows": len(rows), "stale_rows_refreshed": changed}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args()
    print(json.dumps(refresh(ledger_path=args.ledger, out_path=args.out), sort_keys=True))


if __name__ == "__main__":
    main()
