import csv
import json

from append_cache_recovery_receipt import append_receipt


def test_append_uses_explicit_team_week_bridge_metadata(tmp_path):
    fields = [
        "record_type", "receipt_run_id", "canonical_cache_key", "receipt_json_sha256",
        "artifact_id", "artifact", "workflow_run_id", "dispositions",
        "candidate_delta_rows", "candidate_fields", "candidate_file_count", "candidate_files",
        "source_cells", "cache_match_cells", "cache_missing_cells", "cache_conflict_cells",
        "unmatched_cache_cells", "blocked_schema_cells", "final_status", "final_reason",
        "next_action", "identity_profile_run_id", "identity_profile_source",
        "matched_source_player_rows", "identity_bridge_status",
        "bridge_evidence_file_count", "bridge_evidence_row_count", "bridge_evidence_strength",
        "bridge_evidence_schemas", "bridge_evidence_receipt_run_id", "loss_tie_gate",
        "player_team_bridge_gate", "source_precedence_gate", "cache_admission_state",
        "cache_join_strategy",
    ]
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
    readback = tmp_path / "readback.json"
    readback.write_text(json.dumps({
        "canonical_cache_key": "key",
        "candidate_run_id": "1",
        "candidate_rows": 2,
        "candidate_artifacts": 1,
        "source_artifact_id": "source",
        "candidate_fields": ["loss", "tie"],
        "promotion_disposition": "exact_existing_player_null_fill",
        "matched_player_rows": 2,
        "unmatched_candidate_keys": 0,
        "loss_remaining_null_source_cells": 0,
        "tie_remaining_null_source_cells": 0,
        "schema_unchanged": True,
        "player_rows_unchanged": True,
        "ops_unchanged": True,
        "fresh_restore": True,
        "new_lineage": False,
        "identity_bridge_status": "verified_manager_team_week_to_existing_null_player_rows",
        "cache_join_strategy": "db_name|year|week|manager|platform|team_key|team_name|NFL_player_id_is_null",
    }))

    out = tmp_path / "out.csv"
    append_receipt(
        ledger_path=ledger, readback_path=readback, out_path=out,
        receipt_run_id="2", artifact_label="receipt", source_artifact="source",
    )
    with out.open(newline="") as handle:
        row = list(csv.DictReader(handle))[0]
    assert row["identity_bridge_status"] == "verified_manager_team_week_to_existing_null_player_rows"
    assert row["cache_join_strategy"] == "db_name|year|week|manager|platform|team_key|team_name|NFL_player_id_is_null"
