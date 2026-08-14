import csv
import json

from stage_candidate_comparison_ledger import stage


FIELDS = [
    "record_type", "receipt_run_id", "canonical_cache_key", "receipt_json_sha256", "artifact_id", "artifact",
    "workflow_run_id", "dispositions", "candidate_delta_rows", "candidate_fields", "candidate_file_count",
    "candidate_files", "source_cells", "cache_match_cells", "cache_missing_cells", "cache_conflict_cells",
    "unmatched_cache_cells", "blocked_schema_cells", "final_status", "final_reason", "next_action",
    "loss_tie_gate", "player_team_bridge_gate", "source_precedence_gate", "cache_admission_state",
]


def _row():
    return {field: "" for field in FIELDS} | {
        "record_type": "artifact_inventory", "artifact_id": "7", "artifact": "source", "workflow_run_id": "1",
        "canonical_cache_key": "approved", "candidate_delta_rows": "0", "source_cells": "0",
        "cache_match_cells": "0", "cache_missing_cells": "0", "cache_conflict_cells": "0",
        "unmatched_cache_cells": "0", "blocked_schema_cells": "0", "final_status": "unmatched_cache_key",
        "final_reason": "old", "next_action": "old", "loss_tie_gate": "not_required",
        "player_team_bridge_gate": "open_raw_roster_bridge_required", "source_precedence_gate": "not_required",
        "cache_admission_state": "open_player_team_bridge",
    }


def test_stage_keeps_safe_candidate_fills_open_until_cache_readback(tmp_path):
    ledger, receipt, out = tmp_path / "ledger.csv", tmp_path / "receipt.json", tmp_path / "out.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS); writer.writeheader(); writer.writerow(_row())
    receipt.write_text(json.dumps({"canonical_cache_key": "approved", "cache_mutated": False, "new_lineage": False, "by_artifact": {
        "7": {"candidate_cells": 10, "safe_null_fills": {"win": 2}, "same_values": {"win": 3}, "conflicts": {"team_points": 1}, "unmatched_candidates": 4, "blocked": {}},
    }}))

    stage(ledger_path=ledger, comparison_path=receipt, receipt_run_id="100", out_path=out)

    row = next(csv.DictReader(out.open(newline="", encoding="utf-8")))
    assert row["final_status"] == "candidate_comparison_pending_apply"
    assert row["candidate_delta_rows"] == "2"
    assert row["cache_match_cells"] == "3"
    assert row["cache_missing_cells"] == "2"
    assert row["cache_conflict_cells"] == "1"
    assert row["unmatched_cache_cells"] == "4"
    assert row["next_action"] == "apply_safe_null_fills_then_independent_readback; preserve_conflicts_pending_source_precedence_adjudication; reconcile_source_identity_or_close_source_only"


def test_stage_closes_same_value_only_candidate_with_fresh_readonly_receipt(tmp_path):
    ledger, receipt, out = tmp_path / "ledger.csv", tmp_path / "receipt.json", tmp_path / "out.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS); writer.writeheader(); writer.writerow(_row())
    receipt.write_text(json.dumps({"canonical_cache_key": "approved", "cache_mutated": False, "new_lineage": False, "by_artifact": {
        "7": {"candidate_cells": 3, "safe_null_fills": {}, "same_values": {"win": 3}, "conflicts": {}, "unmatched_candidates": 0, "blocked": {}},
    }}))

    stage(ledger_path=ledger, comparison_path=receipt, receipt_run_id="100", out_path=out)

    row = next(csv.DictReader(out.open(newline="", encoding="utf-8")))
    assert row["final_status"] == "cache_verified"
    assert row["next_action"] == "closed_independent_cache_readback"
