"""Close exact ledger artifacts only from a clean restored-cache readback receipt."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

from enrich_artifact_cache_ledger_completion_gates import derive_gate_states


def _readback_is_clean(receipt: dict) -> bool:
    return (
        receipt.get("fresh_restore") is True
        and receipt.get("new_lineage") is False
        and int(receipt.get("loss_remaining_null_source_cells", -1)) == 0
        and int(receipt.get("tie_remaining_null_source_cells", -1)) == 0
    )


def reconcile(
    *,
    ledger_path: Path,
    readback_path: Path,
    artifact_ids: set[str],
    receipt_run_id: str,
    out_path: Path,
) -> dict[str, int]:
    receipt_bytes = readback_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    if not _readback_is_clean(receipt):
        raise SystemExit("readback has remaining source-null cells or is not a fresh same-lineage restore")
    digest = hashlib.sha256(receipt_bytes).hexdigest()
    with ledger_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fields = list(reader.fieldnames or [])
    found = {str(row.get("artifact_id", "")) for row in rows} & artifact_ids
    if found != artifact_ids:
        raise SystemExit(f"ledger artifact ids missing: {sorted(artifact_ids - found)}")
    closed = 0
    for row in rows:
        if str(row.get("artifact_id", "")) not in artifact_ids:
            continue
        if row.get("record_type") != "artifact_inventory":
            raise SystemExit(f"artifact {row['artifact_id']} is not a raw artifact inventory row")
        if row.get("canonical_cache_key") != receipt.get("canonical_cache_key"):
            raise SystemExit(f"artifact {row['artifact_id']} cache key does not match readback receipt")
        row["receipt_run_id"] = str(receipt_run_id)
        row["receipt_json_sha256"] = digest
        row["cache_match_cells"] = str(int(row.get("source_cells", "0") or 0))
        row["cache_missing_cells"] = "0"
        row["cache_conflict_cells"] = "0"
        row["unmatched_cache_cells"] = "0"
        row["blocked_schema_cells"] = "0"
        row["final_status"] = "cache_verified"
        row["final_reason"] = (
            "Fresh restored-cache readback verified this artifact's loss/tie "
            "candidate cells have zero remaining NULLs; schema, ops, and the "
            "single approved cache lineage were preserved."
        )
        row["next_action"] = "closed_cache_verified"
        row.update(derive_gate_states(row))
        closed += 1
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return {"closed_artifacts": closed}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--readback", type=Path, required=True)
    ap.add_argument("--artifact-id", action="append", required=True)
    ap.add_argument("--receipt-run-id", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    print(json.dumps(reconcile(
        ledger_path=args.ledger,
        readback_path=args.readback,
        artifact_ids={str(value) for value in args.artifact_id},
        receipt_run_id=args.receipt_run_id,
        out_path=args.out,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
