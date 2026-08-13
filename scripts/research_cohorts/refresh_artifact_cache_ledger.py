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

from enrich_artifact_cache_ledger_completion_gates import derive_gate_states


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
            "artifact_inventory", "artifact_admission", "cache_recovery_receipt"
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
    team_points_bridge_receipt = "artifact_bridge_state" in receipt
    team_manager_comparison_receipt = "artifact_comparison_state" in receipt
    unattributable_source_receipt = "artifact_unattributable_state" in receipt
    if promotion_receipt:
        receipt_rows = {int(row["artifact_id"]): row for row in receipt["artifact_rows"]}
    elif unattributable_source_receipt:
        # This is a stronger terminal classification than the general direct
        # re-audit: source lacks the fantasy-team identity required to choose
        # among duplicate cache rows.
        receipt_rows = {int(row["artifact_id"]): row for row in receipt["artifact_unattributable_state"]}
    elif direct_reaudit_receipt:
        receipt_rows = {int(row["artifact_id"]): row for row in receipt["artifact_current_state"]}
    elif team_points_bridge_receipt:
        receipt_rows = {int(row["artifact_id"]): row for row in receipt["artifact_bridge_state"]}
    elif team_manager_comparison_receipt:
        receipt_rows = {int(row["artifact_id"]): row for row in receipt["artifact_comparison_state"]}
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
        if unattributable_source_receipt:
            keys = int(evidence.get("unattributable_ambiguous_keys", 0) or 0)
            cells = int(evidence.get("unattributable_source_cells", 0) or 0)
            if keys <= 0 or cells <= 0:
                raise SystemExit(f"{artifact_id}: no unattributable source evidence")
            if evidence.get("all_ambiguous_source_keys_lack_manager") is not True:
                raise SystemExit(f"{artifact_id}: source manager/franchise identity is not proven absent")
            row["source_cells"] = str(cells)
            row["cache_match_cells"] = "0"
            row["cache_missing_cells"] = "0"
            row["cache_conflict_cells"] = "0"
            row["unmatched_cache_cells"] = "0"
            row["blocked_schema_cells"] = "0"
            row["receipt_run_id"] = str(receipt_run_id)
            row["receipt_json_sha256"] = digest
            row["final_status"] = "no_promotable_candidate_emitted"
            row["final_reason"] = (
                f"Current read-only audit found {keys:,} source player-week keys with outcome values but no "
                "source manager/franchise identity. Each maps only to duplicate canonical rows whose manager, "
                "MFL player ID, team key/name, and lineup slot cannot be selected from the source, so no "
                "player-team recipient can be proven. The source values are deliberately not promoted or used "
                "to corroborate cache cells."
            )
            row["next_action"] = "closed_source_identity_absent_no_recipient"
            updated += 1
            continue
        if team_manager_comparison_receipt:
            fields = ("win", "team_points", "is_playoffs", "champion")
            fills = evidence.get("null_fill_cells_by_field", {})
            comparisons = evidence.get("comparison_cells_by_field", {})
            if int(evidence.get("delta_rows", 0) or 0) != 0 or any(
                int(fills.get(field, 0) or 0) != 0 for field in fields
            ):
                raise SystemExit(
                    f"{artifact_id}: manager signal audit has safe NULL fills; "
                    "build and validate an exact candidate before refreshing the ledger"
                )
            for field in fields:
                comparison = comparisons.get(field)
                if comparison is None:
                    raise SystemExit(f"{artifact_id}: missing {field} comparison evidence")
                source_non_null = int(comparison.get("source_non_null", 0) or 0)
                equal = int(comparison.get("cache_equal", 0) or 0)
                conflict = int(comparison.get("cache_conflict", 0) or 0)
                cache_null = int(comparison.get("cache_null", 0) or 0)
                if cache_null != 0 or source_non_null != equal + conflict + cache_null:
                    raise SystemExit(f"{artifact_id}: {field} comparison does not reconcile")
            total_source = sum(int(comparisons[field].get("source_non_null", 0) or 0) for field in fields)
            total_equal = sum(int(comparisons[field].get("cache_equal", 0) or 0) for field in fields)
            conflicts_by_field = {
                field: int(comparisons[field].get("cache_conflict", 0) or 0)
                for field in fields
            }
            total_conflicts = sum(conflicts_by_field.values())
            row["source_cells"] = str(total_source)
            row["cache_match_cells"] = str(total_equal)
            row["cache_missing_cells"] = "0"
            row["cache_conflict_cells"] = str(total_conflicts)
            row["unmatched_cache_cells"] = "0"
            row["receipt_run_id"] = str(receipt_run_id)
            row["receipt_json_sha256"] = digest
            if total_conflicts == 0:
                row["final_status"] = "cache_verified"
                row["final_reason"] = (
                    "Current strict manager-week fan-out audit found no cache-null fills and all "
                    f"{total_source:,} source-backed supported cells already equal the canonical cache."
                )
                row["next_action"] = "closed_independent_cache_readback"
            elif conflicts_by_field["champion"] == total_conflicts:
                row["final_status"] = "cache_conflict_preserved"
                row["final_reason"] = (
                    "Team-level championship flags cannot be promoted as player championship-start credit. "
                    f"Current strict manager-week audit found {total_equal:,} equal cells, no cache-null "
                    f"fills, and {total_conflicts:,} champion-only conflicts; the non-champion fields agree."
                )
                row["next_action"] = "require_championship_start_evidence_not_team_champion_flag"
            else:
                row["final_status"] = "partial_schema_blocked_conflicts"
                row["final_reason"] = (
                    "Current strict manager-week audit found no cache-null fill, but retains conflicting "
                    "non-championship source values that require source-precedence adjudication."
                )
                row["next_action"] = "preserve_conflicts_pending_source_precedence_adjudication"
            updated += 1
            continue
        if team_points_bridge_receipt:
            unique = int(evidence.get("unique_team_points_bridges", 0) or 0)
            if unique:
                raise SystemExit(
                    f"{artifact_id}: team-points bridge found {unique} safe recipients; "
                    "build and validate an exact candidate before refreshing the ledger"
                )
            source_keys = int(evidence.get("manager_null_source_keys_with_team_points", 0) or 0)
            unmatched = int(evidence.get("unmatched_team_points_bridges", 0) or 0)
            ambiguous = int(evidence.get("ambiguous_team_points_bridges", 0) or 0)
            if source_keys != unmatched + ambiguous:
                raise SystemExit(f"{artifact_id}: team-points bridge metrics do not reconcile")
            player_absent = int(evidence.get("canonical_player_absent", 0) or 0)
            only_unassigned = int(evidence.get("canonical_only_unassigned", 0) or 0)
            different_team_points = int(evidence.get("canonical_same_player_different_team_points", 0) or 0)
            null_team_points = int(evidence.get("canonical_same_player_null_team_points", 0) or 0)
            if any((player_absent, only_unassigned, different_team_points, null_team_points)) and (
                player_absent + only_unassigned + different_team_points + null_team_points != unmatched
            ):
                raise SystemExit(f"{artifact_id}: direct-identity classification does not reconcile")
            row["receipt_run_id"] = str(receipt_run_id)
            row["receipt_json_sha256"] = digest
            row["final_status"] = "partial_schema_blocked_direct_identity"
            if only_unassigned == source_keys:
                row["final_reason"] = (
                    f"Current team-points bridge audit found all {source_keys:,} exact player-week keys exist only "
                    "as unassigned/unrostered canonical rows. The source rows also lack a manager/franchise "
                    "identity, so team totals cannot identify a fantasy-team recipient."
                )
                row["next_action"] = "recover_source_franchise_identity_or_close_source_only"
            else:
                row["final_reason"] = (
                    f"Current team-points bridge audit tested {source_keys:,} manager-null source player-weeks; "
                    f"{unmatched:,} had no canonical row with the same league/week/player and team total, "
                    f"and {ambiguous:,} had non-unique team-total matches. No safe recipient exists for this bridge."
                )
                row["next_action"] = "resolve_direct_identity_or_close_source_only"
            updated += 1
            continue
        if direct_reaudit_receipt:
            if int(evidence.get("safe_null_candidates", 0) or 0) != 0:
                raise SystemExit(
                    f"{artifact_id}: direct re-audit has safe null candidates; "
                    "build an exact promotion candidate before refreshing the ledger"
                )
            source_cells = int(evidence.get("source_cells", 0) or 0)
            if source_cells <= 0:
                source_cells = sum(
                    int(evidence.get(field, 0) or 0)
                    for field in (
                        "cache_equal_cells", "safe_null_candidates",
                        "cache_conflict_cells", "ambiguous_null_cells",
                    )
                )
            if source_cells <= 0:
                raise SystemExit(f"{artifact_id}: direct re-audit has no source-cell count")
            row["source_cells"] = str(source_cells)
            row["cache_match_cells"] = str(int(evidence.get("cache_equal_cells", 0) or 0))
            row["cache_missing_cells"] = "0"
            # A direct re-audit that compared canonical loss/tie values has
            # superseded the historical pre-schema loss/tie blocker.
            row["blocked_schema_cells"] = "0"
            row["cache_conflict_cells"] = str(int(evidence.get("cache_conflict_cells", 0) or 0))
            row["unmatched_cache_cells"] = str(int(evidence.get("ambiguous_null_cells", 0) or 0))
            row["receipt_run_id"] = str(receipt_run_id)
            row["receipt_json_sha256"] = digest
            equal = int(evidence.get("cache_equal_cells", 0) or 0)
            ambiguous = int(evidence.get("ambiguous_null_cells", 0) or 0)
            conflicts = int(evidence.get("cache_conflict_cells", 0) or 0)
            if ambiguous == 0 and conflicts == 0:
                row["final_status"] = "cache_verified"
                row["final_reason"] = (
                    "Current exact-player re-audit found no source-backed cache-null candidates, "
                    f"no conflicts, and all {equal:,} supported cells already equal the canonical cache."
                )
                row["next_action"] = "closed_independent_cache_readback"
            else:
                row["final_status"] = "partial_schema_blocked_direct_identity"
                profile = receipt.get("ambiguous_recipient_profile") or {}
                profile_rows = int(profile.get("recipient_rows", 0) or 0)
                identity_free = profile_rows > 0 and all(
                    int(profile.get(field, 0) or 0) == profile_rows
                    for field in (
                        "rows_without_canonical_manager",
                        "rows_without_canonical_mfl_player_id",
                        "rows_without_canonical_team_key",
                        "rows_without_canonical_team_name",
                        "rows_without_canonical_fantasy_position",
                    )
                )
                if identity_free:
                    row["final_reason"] = (
                        "Current exact-player re-audit found cache-grain identity loss: "
                        f"{int(profile.get('keys', 0) or 0):,} duplicate player-week keys fan out to "
                        f"{profile_rows:,} canonical rows, and every recipient lacks manager, MFL player ID, "
                        "team key/name, and lineup slot. Source outcome evidence therefore has no safe "
                        "row recipient despite exact league/week/NFL-player overlap."
                    )
                    row["next_action"] = "reconstruct_mfl_player_team_identity_before_outcome_upsert"
                else:
                    row["final_reason"] = (
                        "Current exact-player re-audit found no safe cache-null candidate; "
                        f"{equal:,} supported cells already match, {ambiguous:,} null cells require "
                        "identity resolution, and "
                        f"{conflicts:,} non-null cells require source-precedence adjudication."
                    )
                    row["next_action"] = (
                        "resolve_direct_identity_and_precedence_adjudication"
                        if conflicts else "resolve_direct_identity_or_close_source_only"
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
    # Gate states are derived from the fresh receipt counts.  Keeping a
    # historical gate after its current count reached zero reopens completed
    # schema work and makes the ledger drift from the cache evidence.
    for row in ledger_rows:
        row.update(derive_gate_states(row))
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
