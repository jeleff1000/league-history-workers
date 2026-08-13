import csv
import json
from pathlib import Path


FIELDS = [
    "record_type", "receipt_run_id", "canonical_cache_key", "receipt_json_sha256",
    "artifact_id", "artifact", "workflow_run_id", "dispositions",
    "candidate_delta_rows", "candidate_fields", "candidate_file_count",
    "candidate_files", "source_cells", "cache_match_cells", "cache_missing_cells",
    "cache_conflict_cells", "unmatched_cache_cells", "blocked_schema_cells",
    "final_status", "final_reason", "next_action", "identity_profile_run_id",
    "identity_profile_source", "source_team_keys", "matched_source_team_keys",
    "unmatched_source_team_keys", "league_week_overlap_keys", "league_week_absent_keys",
    "matched_source_player_rows", "identity_bridge_status", "matched_by_mfl_manager_guid",
    "matched_by_manager_team_name", "matched_by_unique_manager", "cache_join_strategy",
    "bridge_evidence_file_count", "bridge_evidence_row_count", "bridge_evidence_strength",
    "bridge_evidence_schemas", "bridge_evidence_receipt_run_id", "loss_tie_gate",
    "player_team_bridge_gate", "source_precedence_gate", "cache_admission_state",
]


def _clean_readback() -> dict:
    return {
        "candidate_artifacts": 256,
        "candidate_rows": 26936,
        "candidate_run_id": "31647988031",
        "canonical_cache_key": "approved-key",
        "fresh_restore": True,
        "matched_player_rows": 26936,
        "new_lineage": False,
        "ops_unchanged": True,
        "player_rows_unchanged": True,
        "remaining_source_backed_nulls": {
            "champion": 0, "final_playoff_seed": 0, "is_playoffs": 0,
            "loss": 0, "made_playoffs": 0, "team_points": 0, "tie": 0, "win": 0,
        },
        "schema_unchanged": True,
        "source_artifact_id": "8952555354",
        "unmatched_candidate_keys": 0,
    }


def test_append_receipt_preserves_raw_artifact_and_records_clean_readback(tmp_path: Path) -> None:
    from scripts.research_cohorts.append_cache_recovery_receipt import append_receipt

    ledger = tmp_path / "ledger.csv"
    raw = dict.fromkeys(FIELDS, "")
    raw.update({
        "record_type": "artifact_inventory", "receipt_run_id": "old-run",
        "canonical_cache_key": "approved-key", "receipt_json_sha256": "old-sha",
        "artifact_id": "8952555354", "artifact": "raw-source", "workflow_run_id": "old-run",
        "dispositions": "raw", "source_cells": "10", "cache_match_cells": "0",
        "unmatched_cache_cells": "10", "final_status": "unmatched_cache_key",
        "final_reason": "residual source-only cells", "next_action": "bridge",
        "loss_tie_gate": "not_required", "player_team_bridge_gate": "open_raw_roster_bridge_required",
        "source_precedence_gate": "not_required", "cache_admission_state": "open_player_team_bridge",
    })
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerow(raw)

    readback = tmp_path / "readback.json"
    readback.write_text(json.dumps(_clean_readback()), encoding="utf-8")
    out = tmp_path / "out.csv"

    result = append_receipt(
        ledger_path=ledger,
        readback_path=readback,
        out_path=out,
        receipt_run_id="31655409805",
        artifact_label="mfl-source-only-bridge-26936-31655409805",
        source_artifact="research-source-matchup-rescue-3-31062083630",
    )

    assert result == {"ledger_rows": 2, "cache_recovery_receipt_rows": 1}
    rows = list(csv.DictReader(out.open(newline="", encoding="utf-8")))
    assert rows[0] == raw
    receipt = rows[1]
    assert receipt["record_type"] == "cache_recovery_receipt"
    assert receipt["artifact_id"] == "mfl-source-only-bridge-26936-31655409805"
    assert receipt["candidate_delta_rows"] == "26936"
    assert receipt["matched_source_player_rows"] == "26936"
    assert receipt["final_status"] == "cache_verified"
    assert receipt["cache_admission_state"] == "closed_cache_verified"
    assert receipt["candidate_file_count"] == "256"
    assert receipt["candidate_fields"] == (
        "champion|final_playoff_seed|is_playoffs|loss|made_playoffs|team_points|tie|win"
    )


def test_append_receipt_rejects_readback_with_unresolved_nulls(tmp_path: Path) -> None:
    from scripts.research_cohorts.append_cache_recovery_receipt import append_receipt

    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
    readback = tmp_path / "readback.json"
    bad = _clean_readback()
    bad["remaining_source_backed_nulls"]["win"] = 1
    readback.write_text(json.dumps(bad), encoding="utf-8")

    try:
        append_receipt(
            ledger_path=ledger,
            readback_path=readback,
            out_path=tmp_path / "out.csv",
            receipt_run_id="31655409805",
            artifact_label="bad",
            source_artifact="source",
        )
    except ValueError as exc:
        assert "remaining_source_backed_nulls" in str(exc)
    else:
        raise AssertionError("expected unsafe readback rejection")


def test_append_receipt_records_verified_direct_mfl_win_replacements(tmp_path: Path) -> None:
    from scripts.research_cohorts.append_cache_recovery_receipt import append_receipt

    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        csv.DictWriter(handle, fieldnames=FIELDS).writeheader()
    readback = tmp_path / "readback.json"
    readback.write_text(json.dumps({
        "candidate_artifacts": 231,
        "candidate_rows": 451,
        "candidate_run_id": "31663728938",
        "canonical_cache_key": "approved-key",
        "fresh_restore": True,
        "matched_player_rows": 451,
        "new_lineage": False,
        "ops_unchanged": True,
        "player_rows_unchanged": True,
        "schema_unchanged": True,
        "source_artifact_id": "mfl-classification-roster-bridge-batch",
        "unmatched_candidate_keys": 0,
        "source_value_disagreements": {"win": 0, "team_points": 0},
        "promotion_disposition": "exact_existing_player_null_fill|direct_mfl_win_replacement",
        "candidate_fields": ["team_points", "win"],
    }), encoding="utf-8")

    out = tmp_path / "out.csv"
    append_receipt(
        ledger_path=ledger,
        readback_path=readback,
        out_path=out,
        receipt_run_id="31670000000",
        artifact_label="mfl-classification-roster-bridge-batch-31670000000",
        source_artifact="mfl classification roster bridge batch",
    )

    receipt = list(csv.DictReader(out.open(newline="", encoding="utf-8")))[0]
    assert receipt["dispositions"] == "exact_existing_player_null_fill|direct_mfl_win_replacement"
    assert receipt["candidate_fields"] == "team_points|win"
    assert "direct MFL win replacements" in receipt["final_reason"]


def test_append_receipt_accepts_exact_team_null_readback_shape(tmp_path: Path) -> None:
    from scripts.research_cohorts.append_cache_recovery_receipt import append_receipt

    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        csv.DictWriter(handle, fieldnames=FIELDS).writeheader()
    readback = tmp_path / "readback.json"
    readback.write_text(json.dumps({
        "canonical_cache_key": "approved-key",
        "candidate_run_id": "31668497497",
        "source_artifact_id": "9016453846|9016485043",
        "candidate_rows": 600,
        "candidate_unique_keys": 600,
        "matched_player_rows": 600,
        "unmatched_candidate_keys": 0,
        "loss_remaining_null_source_cells": 0,
        "tie_remaining_null_source_cells": 0,
        "schema_unchanged": True,
        "player_rows_unchanged": True,
        "ops_unchanged": True,
        "fresh_restore": True,
        "new_lineage": False,
    }), encoding="utf-8")

    out = tmp_path / "out.csv"
    append_receipt(
        ledger_path=ledger,
        readback_path=readback,
        out_path=out,
        receipt_run_id="31669704187",
        artifact_label="exact-team-null-promotion-600-31669704187",
        source_artifact="research-sparse-playoff-championship-rescue-46|94",
    )

    receipt = list(csv.DictReader(out.open(newline="", encoding="utf-8")))[0]
    assert receipt["candidate_file_count"] == "2"
    assert receipt["candidate_fields"] == "loss|tie"
    assert receipt["final_status"] == "cache_verified"


def test_append_receipt_accepts_labeled_legacy_apply_receipt(tmp_path: Path) -> None:
    from scripts.research_cohorts.append_cache_recovery_receipt import append_receipt

    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        csv.DictWriter(handle, fieldnames=FIELDS).writeheader()
    readback = tmp_path / "readback.json"
    payload = _clean_readback()
    payload.update({
        "readback_proof_mode": "legacy_apply_receipt_minimum_match",
        "authorized_cells_by_field": {"win": 2},
        "source_value_disagreements": {"win": 7},
        "source_artifact_id": "mfl-legacy-batch",
    })
    readback.write_text(json.dumps(payload), encoding="utf-8")

    out = tmp_path / "out.csv"
    append_receipt(
        ledger_path=ledger,
        readback_path=readback,
        out_path=out,
        receipt_run_id="31682222659",
        artifact_label="mfl-legacy",
        source_artifact="mfl saved batch",
    )

    receipt = list(csv.DictReader(out.open(newline="", encoding="utf-8")))[0]
    assert "legacy apply receipt" in receipt["final_reason"]
