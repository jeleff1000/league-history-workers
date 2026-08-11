"""Overlay a direct source-readback receipt onto the durable artifact ledger.

This only updates the CSV evidence ledger.  It never opens or mutates a
research cache.  Every changed ledger row retains the source Action receipt
that supplied its proof.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path


def _next_action(receipt: dict) -> str:
    if int(receipt.get("cache_missing_cells", 0) or 0):
        if int(receipt.get("blocked_schema_cells", 0) or 0):
            return "apply_supported_null_cell_updates; separately_adjudicate_blocked_schema_fields"
        return "apply_strict_null_cell_updates_then_readback"
    if int(receipt.get("cache_conflict_cells", 0) or 0):
        return "preserve_conflicts_pending_source_precedence_adjudication"
    if int(receipt.get("unmatched_cache_cells", 0) or 0):
        return "reconcile_source_identity_or_close_source_only"
    return "closed_cache_verified"


def refresh(
    *, ledger_path: Path, receipt_path: Path, selected_path: Path,
    receipt_run_id: str, out_path: Path,
) -> dict[str, int]:
    ledger_rows = list(csv.DictReader(ledger_path.open(newline="", encoding="utf-8")))
    if not ledger_rows:
        raise SystemExit("ledger is empty")
    ids = [int(row["artifact_id"]) for row in ledger_rows]
    if len(ids) != len(set(ids)):
        raise SystemExit("ledger has duplicate artifact IDs")
    selected = json.loads(selected_path.read_text(encoding="utf-8"))
    selected_ids = {int(row["artifact_id"]) for row in selected}
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt_rows = {int(row["artifact_id"]): row for row in receipt["rows"]}
    if not selected_ids <= set(receipt_rows):
        raise SystemExit("selected artifact is absent from receipt")
    digest = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    updated = 0
    for row in ledger_rows:
        artifact_id = int(row["artifact_id"])
        if artifact_id not in selected_ids:
            continue
        evidence = receipt_rows[artifact_id]
        if int(evidence.get("source_cells", 0) or 0) == 0:
            raise SystemExit(f"{artifact_id}: selected artifact has no readback source cells")
        for field in (
            "source_cells", "cache_match_cells", "cache_missing_cells",
            "cache_conflict_cells", "unmatched_cache_cells", "blocked_schema_cells",
        ):
            row[field] = str(int(evidence.get(field, 0) or 0))
        row["receipt_run_id"] = str(receipt_run_id)
        row["receipt_json_sha256"] = digest
        row["final_status"] = str(evidence["final_status"])
        row["final_reason"] = str(evidence["final_reason"])
        row["next_action"] = _next_action(evidence)
        updated += 1
    if updated != len(selected_ids):
        raise SystemExit(f"updated {updated} ledger rows for {len(selected_ids)} selected artifacts")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(ledger_rows[0]))
        writer.writeheader()
        writer.writerows(ledger_rows)
    return {"ledger_rows": len(ledger_rows), "updated_rows": updated}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--receipt", type=Path, required=True)
    ap.add_argument("--selected", type=Path, required=True)
    ap.add_argument("--receipt-run-id", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    print(json.dumps(refresh(
        ledger_path=args.ledger, receipt_path=args.receipt, selected_path=args.selected,
        receipt_run_id=args.receipt_run_id, out_path=args.out,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
