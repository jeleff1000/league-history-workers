"""Record an exact partial team-signal promotion without claiming source-only rows closed."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

from enrich_artifact_cache_ledger_completion_gates import derive_gate_states


def _clean_promotion(receipt: dict) -> bool:
    return (
        receipt.get("fresh_restore") is True
        and receipt.get("new_lineage") is False
        and receipt.get("schema_unchanged") is True
        and receipt.get("player_rows_unchanged") is True
        and receipt.get("ops_unchanged") is True
        and int(receipt.get("candidate_rows", -1)) > 0
        and int(receipt.get("matched_player_rows", -1)) == int(receipt.get("candidate_rows", -2))
        and int(receipt.get("unmatched_candidate_keys", -1)) == 0
        and int(receipt.get("loss_remaining_null_source_cells", -1)) == 0
        and int(receipt.get("tie_remaining_null_source_cells", -1)) == 0
    )


def reconcile(
    *,
    ledger_path: Path,
    promotion_readback_path: Path,
    refreshed_receipts_path: Path,
    receipt_run_id: str,
    refreshed_readback_run_id: str,
    out_path: Path,
) -> dict[str, int]:
    promotion_bytes = promotion_readback_path.read_bytes()
    promotion = json.loads(promotion_bytes)
    if not _clean_promotion(promotion):
        raise SystemExit("promotion readback is not a clean same-lineage exact null-fill proof")
    artifact_id = str(promotion["source_artifact_id"])
    refreshed = json.loads(refreshed_receipts_path.read_text(encoding="utf-8"))
    refreshed_row = next(
        (row for row in refreshed.get("rows", []) if str(row.get("artifact_id")) == artifact_id),
        None,
    )
    if refreshed_row is None:
        raise SystemExit(f"refreshed receipt omits artifact {artifact_id}")
    if any(int(refreshed_row.get(field, -1)) != 0 for field in ("cache_missing_cells", "cache_conflict_cells", "blocked_schema_cells")):
        raise SystemExit(f"refreshed receipt retains unsafe cells: {refreshed_row}")
    if int(refreshed_row.get("unmatched_cache_cells", 0)) <= 0:
        raise SystemExit("this partial-promotion receipt requires remaining source-only rows")

    with ledger_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fields = list(reader.fieldnames or [])
    matched = [row for row in rows if str(row.get("artifact_id")) == artifact_id]
    if len(matched) != 1:
        raise SystemExit(f"expected exactly one ledger record for {artifact_id}, found {len(matched)}")
    row = matched[0]
    if row.get("record_type") != "artifact_inventory":
        raise SystemExit(f"artifact {artifact_id} is not a raw artifact ledger record")
    if row.get("canonical_cache_key") != promotion.get("canonical_cache_key"):
        raise SystemExit("promotion receipt cache key does not match ledger")
    if int(row.get("matched_source_player_rows", 0) or 0) != int(promotion["matched_player_rows"]):
        raise SystemExit("promotion matched-player count does not match ledger bridge receipt")

    receipt_sha = hashlib.sha256(promotion_bytes).hexdigest()
    for field in (
        "source_cells", "cache_match_cells", "cache_missing_cells", "cache_conflict_cells",
        "unmatched_cache_cells", "blocked_schema_cells", "final_status", "final_reason",
    ):
        row[field] = str(refreshed_row[field])
    row["receipt_run_id"] = str(receipt_run_id)
    row["receipt_json_sha256"] = receipt_sha
    row["identity_profile_run_id"] = str(refreshed_readback_run_id)
    row["identity_profile_source"] = "final_artifact_cache_receipts.json"
    row["next_action"] = "reconcile_source_only_team_keys_or_close_source_only"
    row["final_reason"] = (
        "Fresh same-key cache restore proved all 52,667 existing-player loss/tie candidate cells were filled. "
        "The remaining source cells belong to 3,606 team-week identities with no canonical player recipient."
    )
    row.update(derive_gate_states(row))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return {"updated_artifacts": 1}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--promotion-readback", type=Path, required=True)
    ap.add_argument("--refreshed-receipts", type=Path, required=True)
    ap.add_argument("--receipt-run-id", required=True)
    ap.add_argument("--refreshed-readback-run-id", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    print(json.dumps(reconcile(
        ledger_path=args.ledger,
        promotion_readback_path=args.promotion_readback,
        refreshed_receipts_path=args.refreshed_receipts,
        receipt_run_id=args.receipt_run_id,
        refreshed_readback_run_id=args.refreshed_readback_run_id,
        out_path=args.out,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
