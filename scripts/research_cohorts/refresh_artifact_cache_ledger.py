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
    missing = int(receipt.get("cache_missing_cells", 0) or 0)
    blocked = int(receipt.get("blocked_schema_cells", 0) or 0)
    conflicts = int(receipt.get("cache_conflict_cells", 0) or 0)
    unmatched = int(receipt.get("unmatched_cache_cells", 0) or 0)
    actions: list[str] = []
    if missing:
        actions.append("apply_supported_null_cell_updates")
    if blocked:
        actions.append("add_loss_tie_schema_then_readback")
    if conflicts:
        actions.append("preserve_conflicts_pending_source_precedence_adjudication")
    if unmatched:
        actions.append("reconcile_source_identity_or_close_source_only")
    if blocked and unmatched and not missing and not conflicts:
        return "add_loss_tie_schema_then_reconcile_source_identity_or_close_source_only"
    if actions:
        return "; ".join(actions)
    return "closed_cache_verified"


def refresh(
    *, ledger_path: Path, receipt_path: Path, selected_path: Path,
    receipt_run_id: str, out_path: Path, team_profile_path: Path | None = None,
) -> dict[str, int]:
    ledger_rows = list(csv.DictReader(ledger_path.open(newline="", encoding="utf-8")))
    if not ledger_rows:
        raise SystemExit("ledger is empty")
    raw_rows = [
        row for row in ledger_rows
        if row.get("record_type") == "artifact_inventory"
    ]
    unknown_record_types = [
        row.get("record_type", "") for row in ledger_rows
        if row.get("record_type") not in {
            "artifact_inventory", "cache_recovery_receipt"
        }
    ]
    if unknown_record_types:
        raise SystemExit("ledger has an unknown record type")
    ids = [int(row["artifact_id"]) for row in raw_rows]
    if len(ids) != len(set(ids)):
        raise SystemExit("ledger has duplicate artifact IDs")
    selected = json.loads(selected_path.read_text(encoding="utf-8-sig"))
    selected_ids = {int(row["artifact_id"]) for row in selected}
    receipt = json.loads(receipt_path.read_text(encoding="utf-8-sig"))
    promotion_receipt = "artifact_rows" in receipt
    direct_reaudit_receipt = "artifact_current_state" in receipt
    if promotion_receipt:
        receipt_rows = {int(row["artifact_id"]): row for row in receipt["artifact_rows"]}
    elif direct_reaudit_receipt:
        receipt_rows = {int(row["artifact_id"]): row for row in receipt["artifact_current_state"]}
    else:
        # Combined readback receipts include supplemental cache-recovery
        # entries whose IDs are descriptive strings.  This refresher updates
        # frozen raw artifact rows only, so those entries are not candidates
        # for this numeric raw-artifact lookup.
        receipt_rows = {}
        for row in receipt["rows"]:
            try:
                artifact_id = int(row["artifact_id"])
            except (KeyError, TypeError, ValueError):
                continue
            receipt_rows[artifact_id] = row
    team_profile_rows: dict[int, dict] = {}
    if team_profile_path is not None:
        profile = json.loads(team_profile_path.read_text(encoding="utf-8"))
        team_profile_rows = {
            int(row["artifact_id"]): row for row in profile.get("rows", [])
        }
    if not selected_ids <= set(receipt_rows):
        raise SystemExit("selected artifact is absent from receipt")
    digest = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    updated = 0
    for row in ledger_rows:
        if row.get("record_type") != "artifact_inventory":
            continue
        artifact_id = int(row["artifact_id"])
        if artifact_id not in selected_ids:
            continue
        evidence = receipt_rows[artifact_id]
        if direct_reaudit_receipt:
            if int(evidence.get("safe_null_candidates", 0) or 0) != 0:
                raise SystemExit(
                    f"{artifact_id}: direct re-audit has safe null candidates; "
                    "build an exact promotion candidate before refreshing the ledger"
                )
            row["cache_match_cells"] = str(int(evidence.get("cache_equal_cells", 0) or 0))
            row["cache_missing_cells"] = "0"
            row["cache_conflict_cells"] = str(int(evidence.get("cache_conflict_cells", 0) or 0))
            row["unmatched_cache_cells"] = str(int(evidence.get("ambiguous_null_cells", 0) or 0))
            row["receipt_run_id"] = str(receipt_run_id)
            row["receipt_json_sha256"] = digest
            row["final_status"] = "partial_schema_blocked_direct_identity"
            row["final_reason"] = (
                "Current exact-player re-audit found no safe cache-null candidate; "
                f"{int(evidence.get('cache_equal_cells', 0) or 0)} supported cells already match, "
                f"{int(evidence.get('ambiguous_null_cells', 0) or 0)} null cells require identity resolution, "
                f"and {int(evidence.get('cache_conflict_cells', 0) or 0)} non-null cells require "
                "source-precedence adjudication."
            )
            row["next_action"] = (
                "add_loss_tie_schema_then_resolve_direct_identity_and_precedence_adjudication"
            )
            updated += 1
            continue
        if promotion_receipt:
            expected_rows = int(row["candidate_delta_rows"] or 0)
            promoted_rows = int(evidence.get("candidate_rows", 0) or 0)
            if expected_rows != promoted_rows:
                raise SystemExit(
                    f"{artifact_id}: receipt rows {promoted_rows} do not match "
                    f"ledger candidate rows {expected_rows}"
                )
            promoted_cells = sum(
                int(evidence.get(field, 0) or 0)
                for field in ("win_cells", "team_point_cells", "playoff_cells")
            )
            if promoted_cells == 0:
                raise SystemExit(f"{artifact_id}: promotion receipt has no applied cells")
            old_missing = int(row["cache_missing_cells"] or 0)
            if promoted_cells > old_missing:
                raise SystemExit(
                    f"{artifact_id}: receipt applies {promoted_cells} cells but ledger only "
                    f"records {old_missing} missing cells"
                )
            row["cache_match_cells"] = str(
                int(row["cache_match_cells"] or 0) + promoted_cells
            )
            row["cache_missing_cells"] = str(old_missing - promoted_cells)
            row["receipt_run_id"] = str(receipt_run_id)
            row["receipt_json_sha256"] = digest
            row["final_status"] = "partial_schema_blocked_cache_updates"
            row["final_reason"] = (
                "Exact supported candidate promotion verified after canonical-cache restore: "
                f"{promoted_rows} player rows and {promoted_cells} cells "
                f"(win={int(evidence.get('win_cells', 0) or 0)}, "
                f"team_points={int(evidence.get('team_point_cells', 0) or 0)}, "
                f"is_playoffs={int(evidence.get('playoff_cells', 0) or 0)}). "
                "Remaining source evidence is limited to blocked loss/tie schema, "
                "preserved non-null conflicts, or identity ambiguity."
            )
            row["next_action"] = (
                "add_loss_tie_schema_then_reaudit_remaining_direct_source_cells"
            )
            updated += 1
            continue
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
        profile = team_profile_rows.get(artifact_id)
        if profile is not None:
            overlap = int(profile.get("league_week_overlap_keys", 0) or 0)
            matched = int(profile.get("matched_source_team_keys", 0) or 0)
            source_keys = int(profile.get("source_team_keys", 0) or 0)
            absent = int(profile.get("league_week_absent_keys", 0) or 0)
            if overlap > 0 and matched == 0:
                row["final_status"] = "source_only_team_signal_missing_player_team_bridge"
                row["final_reason"] = (
                    f"Raw team signals cover {source_keys} team-weeks; {overlap} overlapping cache "
                    "league-weeks have player rows but no team_key/manager/team_name bridge; "
                    f"{absent} source team-weeks have no cache player rows."
                )
                row["next_action"] = "locate_player_team_roster_bridge_then_exact_player_upsert"
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
    ap.add_argument("--team-profile", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    print(json.dumps(refresh(
        ledger_path=args.ledger, receipt_path=args.receipt, selected_path=args.selected,
        receipt_run_id=args.receipt_run_id, team_profile_path=args.team_profile,
        out_path=args.out,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
