"""Stage read-only candidate-comparison evidence into the sole artifact ledger."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

from enrich_artifact_cache_ledger_completion_gates import derive_gate_states


def _total(values: dict[str, int] | None) -> int:
    return sum(int(value or 0) for value in (values or {}).values())


def _next_action(*, safe: int, conflicts: int, unmatched: int) -> str:
    steps: list[str] = []
    if safe:
        steps.append("apply_safe_null_fills_then_independent_readback")
    if conflicts:
        steps.append("preserve_conflicts_pending_source_precedence_adjudication")
    if unmatched:
        steps.append("reconcile_source_identity_or_close_source_only")
    return "; ".join(steps) or "closed_independent_cache_readback"


def stage(*, ledger_path: Path, comparison_path: Path, receipt_run_id: str, out_path: Path) -> dict[str, int]:
    """Update raw ledger rows from a fresh, cache-immutable comparison receipt."""
    with ledger_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fields = list(reader.fieldnames or [])
        rows = list(reader)
    receipt = json.loads(comparison_path.read_text(encoding="utf-8"))
    if receipt.get("cache_mutated") or receipt.get("new_lineage"):
        raise ValueError("comparison receipt is not read-only")
    if receipt.get("cache_sha256_before") and receipt.get("cache_sha256_before") != receipt.get("cache_sha256_after"):
        raise ValueError("comparison receipt changed the canonical cache")
    canonical_key = str(receipt.get("canonical_cache_key", "") or "")
    evidence = receipt.get("by_artifact") or {}
    digest = hashlib.sha256(comparison_path.read_bytes()).hexdigest()
    raw = {str(row["artifact_id"]): row for row in rows if row.get("record_type") == "artifact_inventory"}
    unknown = sorted(set(evidence) - set(raw))
    if unknown:
        raise ValueError(f"comparison references non-ledger artifacts: {unknown[:10]}")

    updated = 0
    for artifact_id, details in evidence.items():
        row = raw[artifact_id]
        if canonical_key and row.get("canonical_cache_key") and row["canonical_cache_key"] != canonical_key:
            raise ValueError(f"{artifact_id}: receipt canonical cache key does not match ledger")
        safe = _total(details.get("safe_null_fills"))
        same = _total(details.get("same_values"))
        conflicts = _total(details.get("conflicts"))
        unmatched = int(details.get("unmatched_candidates", 0) or 0)
        blocked = _total(details.get("blocked"))
        source = int(details.get("candidate_cells", 0) or 0)
        # Source cells and recipient cells are intentionally different grains:
        # one validated team/manager-week source signal fans out to multiple
        # player-week rows.  Do not force a false 1:1 reconciliation here.
        row.update({
            "receipt_run_id": str(receipt_run_id),
            "receipt_json_sha256": digest,
            "canonical_cache_key": canonical_key or row.get("canonical_cache_key", ""),
            "candidate_delta_rows": str(safe),
            "candidate_fields": "|".join(sorted((details.get("safe_null_fills") or {}).keys())),
            "source_cells": str(source),
            "cache_match_cells": str(same),
            "cache_missing_cells": str(safe),
            "cache_conflict_cells": str(conflicts),
            "unmatched_cache_cells": str(unmatched),
            "blocked_schema_cells": "0",
        })
        if safe or conflicts or unmatched:
            row["final_status"] = "candidate_comparison_pending_apply"
            row["final_reason"] = (
                f"Fresh read-only comparison: {source:,} source-backed candidate cells; {safe:,} safe NULL fills, "
                f"{same:,} existing equal values, {conflicts:,} non-null conflicts, {unmatched:,} no-recipient cells, "
                f"and {blocked:,} semantic source claims quarantined. No cache write has occurred."
            )
        elif blocked:
            row["final_status"] = "cache_conflict_preserved"
            row["final_reason"] = (
                f"Fresh read-only comparison found {blocked:,} semantic source claims that cannot map to player-level "
                "cache truth (for example, a team championship signal). No writable candidate remains."
            )
        else:
            row["final_status"] = "cache_verified"
            row["final_reason"] = (
                f"Fresh read-only comparison found all {same:,} supported source-backed cells already equal the canonical cache; "
                "no cache write was required."
            )
        row["next_action"] = _next_action(safe=safe, conflicts=conflicts, unmatched=unmatched)
        row.update(derive_gate_states(row))
        updated += 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return {"ledger_rows": len(rows), "updated_rows": updated}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--receipt-run-id", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(stage(
        ledger_path=args.ledger, comparison_path=args.comparison,
        receipt_run_id=args.receipt_run_id, out_path=args.out,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
