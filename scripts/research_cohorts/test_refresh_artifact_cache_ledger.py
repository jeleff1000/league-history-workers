import csv
import json

from refresh_artifact_cache_ledger import _next_action, refresh


FIELDS = [
    "record_type", "receipt_run_id", "canonical_cache_key", "receipt_json_sha256", "artifact_id",
    "artifact", "workflow_run_id", "dispositions", "candidate_delta_rows",
    "candidate_fields", "candidate_file_count", "candidate_files", "source_cells",
    "cache_match_cells", "cache_missing_cells", "cache_conflict_cells",
    "unmatched_cache_cells", "blocked_schema_cells", "final_status", "final_reason",
    "next_action", "identity_profile_run_id", "identity_profile_source",
    "source_team_keys", "matched_source_team_keys", "unmatched_source_team_keys",
    "league_week_overlap_keys", "league_week_absent_keys", "matched_source_player_rows",
    "identity_bridge_status", "matched_by_mfl_manager_guid", "matched_by_manager_team_name",
    "matched_by_unique_manager", "cache_join_strategy",
]


def _ledger_row():
    row = {field: "" for field in FIELDS}
    row.update({
        "record_type": "artifact_inventory",
        "receipt_run_id": "old-receipt",
        "canonical_cache_key": "canonical",
        "receipt_json_sha256": "old-sha",
        "artifact_id": "7",
        "artifact": "mfl-source",
        "workflow_run_id": "123",
        "candidate_delta_rows": "9",
        "source_cells": "100",
        "cache_match_cells": "50",
        "cache_missing_cells": "9",
        "cache_conflict_cells": "1",
        "unmatched_cache_cells": "0",
        "blocked_schema_cells": "4",
        "final_status": "partial_schema_blocked_cache_updates",
        "final_reason": "old",
        "next_action": "apply_supported_null_cell_updates",
    })
    return row


def test_refresh_records_current_direct_player_reaudit(tmp_path):
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerow(_ledger_row())
    selected = tmp_path / "selected.json"
    selected.write_text(json.dumps([{"artifact_id": 7}]))
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({
        "artifact_current_state": [{
            "artifact_id": 7,
            "source_rows": 40,
            "cache_equal_cells": 81,
            "safe_null_candidates": 0,
            "cache_conflict_cells": 3,
            "ambiguous_null_cells": 2,
        }],
    }))
    out = tmp_path / "out.csv"

    result = refresh(
        ledger_path=ledger,
        receipt_path=receipt,
        selected_path=selected,
        receipt_run_id="new-receipt",
        out_path=out,
    )

    assert result == {"ledger_rows": 1, "updated_rows": 1}
    row = next(csv.DictReader(out.open(newline="", encoding="utf-8")))
    assert row["cache_match_cells"] == "81"
    assert row["cache_missing_cells"] == "0"
    assert row["cache_conflict_cells"] == "3"
    assert row["unmatched_cache_cells"] == "2"
    assert row["final_status"] == "partial_schema_blocked_direct_identity"
    assert row["next_action"] == "resolve_direct_identity_and_precedence_adjudication"


def test_next_action_keeps_identity_closure_visible_with_loss_tie_schema_work():
    assert _next_action({
        "blocked_schema_cells": 4,
        "cache_missing_cells": 0,
        "cache_conflict_cells": 0,
        "unmatched_cache_cells": 2,
    }) == "add_loss_tie_schema_then_reconcile_source_identity_or_close_source_only"


def test_refresh_ignores_supplemental_cache_recovery_receipts_when_selecting_raw_artifacts(tmp_path):
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerow(_ledger_row())
        recovery = _ledger_row()
        recovery.update({
            "record_type": "cache_recovery_receipt",
            "artifact_id": "mfl-74-row-recovery-31624673691",
            "final_status": "cache_verified",
            "cache_missing_cells": "0",
            "cache_conflict_cells": "0",
            "unmatched_cache_cells": "0",
            "blocked_schema_cells": "0",
        })
        writer.writerow(recovery)
    selected = tmp_path / "selected.json"
    selected.write_text(json.dumps([{"artifact_id": 7}]))
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({
        "artifact_current_state": [{
            "artifact_id": 7,
            "source_rows": 40,
            "cache_equal_cells": 81,
            "safe_null_candidates": 0,
            "cache_conflict_cells": 3,
            "ambiguous_null_cells": 2,
        }],
    }))
    out = tmp_path / "out.csv"

    assert refresh(
        ledger_path=ledger,
        receipt_path=receipt,
        selected_path=selected,
        receipt_run_id="new-receipt",
        out_path=out,
    ) == {"ledger_rows": 2, "updated_rows": 1}
    rows = list(csv.DictReader(out.open(newline="", encoding="utf-8")))
    assert rows[1]["artifact_id"] == "mfl-74-row-recovery-31624673691"
    assert rows[1]["receipt_run_id"] == "old-receipt"


def test_refresh_preserves_artifact_admissions_without_rejecting_the_ledger(tmp_path):
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerow(_ledger_row())
        admission = _ledger_row()
        admission.update({
            "record_type": "artifact_admission",
            "artifact_id": "admission-7",
            "final_status": "cache_verified",
        })
        writer.writerow(admission)
    selected = tmp_path / "selected.json"
    selected.write_text(json.dumps([{"artifact_id": 7}]))
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({
        "artifact_current_state": [{
            "artifact_id": 7,
            "source_rows": 40,
            "cache_equal_cells": 81,
            "safe_null_candidates": 0,
            "cache_conflict_cells": 3,
            "ambiguous_null_cells": 2,
        }],
    }))
    out = tmp_path / "out.csv"

    assert refresh(
        ledger_path=ledger,
        receipt_path=receipt,
        selected_path=selected,
        receipt_run_id="new-receipt",
        out_path=out,
    ) == {"ledger_rows": 2, "updated_rows": 1}
    rows = list(csv.DictReader(out.open(newline="", encoding="utf-8")))
    assert rows[1]["record_type"] == "artifact_admission"
    assert rows[1]["artifact_id"] == "admission-7"
