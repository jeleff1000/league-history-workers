import csv
import json
from pathlib import Path


def test_direct_readback_replaces_stale_source_cell_count(tmp_path: Path) -> None:
    from scripts.research_cohorts.refresh_artifact_cache_ledger import refresh

    fields = [
        "record_type", "artifact_id", "artifact", "workflow_run_id", "receipt_run_id",
        "receipt_json_sha256", "source_cells", "cache_match_cells", "cache_missing_cells",
        "cache_conflict_cells", "unmatched_cache_cells", "blocked_schema_cells",
        "final_status", "final_reason", "next_action",
    ]
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({
            "record_type": "artifact_inventory", "artifact_id": "7", "artifact": "mfl-source",
            "workflow_run_id": "123", "receipt_run_id": "old", "receipt_json_sha256": "old-sha",
            "source_cells": "100", "cache_match_cells": "50", "cache_missing_cells": "9",
            "cache_conflict_cells": "1", "unmatched_cache_cells": "0", "blocked_schema_cells": "0",
            "final_status": "partial_schema_blocked_direct_identity", "final_reason": "old",
            "next_action": "old",
        })
    selected = tmp_path / "selected.json"
    selected.write_text(json.dumps([{"artifact_id": 7}]))
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"artifact_current_state": [{
        "artifact_id": 7,
        "source_rows": 40,
        "source_cells": 86,
        "cache_equal_cells": 81,
        "safe_null_candidates": 0,
        "cache_conflict_cells": 3,
        "ambiguous_null_cells": 2,
    }]}))

    refresh(
        ledger_path=ledger, receipt_path=receipt, selected_path=selected,
        receipt_run_id="new", out_path=tmp_path / "out.csv",
    )

    row = next(csv.DictReader((tmp_path / "out.csv").open(newline="", encoding="utf-8")))
    assert row["source_cells"] == "86"


def test_direct_readback_closes_an_artifact_when_every_source_cell_agrees(tmp_path: Path) -> None:
    from scripts.research_cohorts.refresh_artifact_cache_ledger import refresh

    fields = [
        "record_type", "artifact_id", "artifact", "workflow_run_id", "receipt_run_id",
        "receipt_json_sha256", "source_cells", "cache_match_cells", "cache_missing_cells",
        "cache_conflict_cells", "unmatched_cache_cells", "blocked_schema_cells",
        "final_status", "final_reason", "next_action",
    ]
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({
            "record_type": "artifact_inventory", "artifact_id": "8", "artifact": "mfl-source",
            "workflow_run_id": "123", "receipt_run_id": "old", "receipt_json_sha256": "old-sha",
            "source_cells": "10", "cache_match_cells": "0", "cache_missing_cells": "1",
            "cache_conflict_cells": "0", "unmatched_cache_cells": "0", "blocked_schema_cells": "4",
            "final_status": "partial_schema_blocked_direct_identity", "final_reason": "old",
            "next_action": "old",
        })
    selected = tmp_path / "selected.json"
    selected.write_text(json.dumps([{"artifact_id": 8}]))
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"artifact_current_state": [{
        "artifact_id": 8,
        "source_rows": 2,
        "source_cells": 10,
        "cache_equal_cells": 10,
        "safe_null_candidates": 0,
        "cache_conflict_cells": 0,
        "ambiguous_null_cells": 0,
    }]}))

    refresh(
        ledger_path=ledger, receipt_path=receipt, selected_path=selected,
        receipt_run_id="new", out_path=tmp_path / "out.csv",
    )

    row = next(csv.DictReader((tmp_path / "out.csv").open(newline="", encoding="utf-8")))
    assert row["final_status"] == "cache_verified"
    assert row["next_action"] == "closed_independent_cache_readback"
    assert row["blocked_schema_cells"] == "0"


def test_direct_readback_derives_source_cells_from_a_legacy_receipt(tmp_path: Path) -> None:
    from scripts.research_cohorts.refresh_artifact_cache_ledger import refresh

    fields = [
        "record_type", "artifact_id", "artifact", "workflow_run_id", "receipt_run_id",
        "receipt_json_sha256", "source_cells", "cache_match_cells", "cache_missing_cells",
        "cache_conflict_cells", "unmatched_cache_cells", "blocked_schema_cells",
        "final_status", "final_reason", "next_action",
    ]
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({
            "record_type": "artifact_inventory", "artifact_id": "9", "artifact": "mfl-source",
            "workflow_run_id": "123", "receipt_run_id": "old", "receipt_json_sha256": "old-sha",
            "source_cells": "10", "cache_match_cells": "0", "cache_missing_cells": "1",
            "cache_conflict_cells": "0", "unmatched_cache_cells": "0", "blocked_schema_cells": "0",
            "final_status": "partial_schema_blocked_direct_identity", "final_reason": "old",
            "next_action": "old",
        })
    selected = tmp_path / "selected.json"
    selected.write_text(json.dumps([{"artifact_id": 9}]))
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"artifact_current_state": [{
        "artifact_id": 9,
        "source_rows": 2,
        "cache_equal_cells": 8,
        "safe_null_candidates": 0,
        "cache_conflict_cells": 0,
        "ambiguous_null_cells": 2,
    }]}))

    refresh(
        ledger_path=ledger, receipt_path=receipt, selected_path=selected,
        receipt_run_id="new", out_path=tmp_path / "out.csv",
    )

    row = next(csv.DictReader((tmp_path / "out.csv").open(newline="", encoding="utf-8")))
    assert row["source_cells"] == "10"


def test_direct_readback_records_cache_grain_loss_when_all_duplicate_recipients_lack_identity(tmp_path: Path) -> None:
    from scripts.research_cohorts.refresh_artifact_cache_ledger import refresh

    fields = [
        "record_type", "artifact_id", "artifact", "workflow_run_id", "receipt_run_id",
        "receipt_json_sha256", "source_cells", "cache_match_cells", "cache_missing_cells",
        "cache_conflict_cells", "unmatched_cache_cells", "blocked_schema_cells",
        "final_status", "final_reason", "next_action",
    ]
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({
            "record_type": "artifact_inventory", "artifact_id": "10", "artifact": "mfl-source",
            "workflow_run_id": "123", "receipt_run_id": "old", "receipt_json_sha256": "old-sha",
            "source_cells": "10", "cache_match_cells": "0", "cache_missing_cells": "1",
            "cache_conflict_cells": "0", "unmatched_cache_cells": "0", "blocked_schema_cells": "0",
            "final_status": "partial_schema_blocked_direct_identity", "final_reason": "old",
            "next_action": "old",
        })
    selected = tmp_path / "selected.json"
    selected.write_text(json.dumps([{"artifact_id": 10}]))
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({
        "ambiguous_recipient_profile": {
            "keys": 1, "recipient_rows": 3,
            "rows_without_canonical_manager": 3,
            "rows_without_canonical_mfl_player_id": 3,
            "rows_without_canonical_team_key": 3,
            "rows_without_canonical_team_name": 3,
            "rows_without_canonical_fantasy_position": 3,
        },
        "artifact_current_state": [{
            "artifact_id": 10, "source_rows": 2, "source_cells": 10,
            "cache_equal_cells": 5, "safe_null_candidates": 0,
            "cache_conflict_cells": 0, "ambiguous_null_cells": 5,
        }],
    }))

    refresh(
        ledger_path=ledger, receipt_path=receipt, selected_path=selected,
        receipt_run_id="new", out_path=tmp_path / "out.csv",
    )

    row = next(csv.DictReader((tmp_path / "out.csv").open(newline="", encoding="utf-8")))
    assert row["final_status"] == "partial_schema_blocked_direct_identity"
    assert row["next_action"] == "reconstruct_mfl_player_team_identity_before_outcome_upsert"
    assert "cache-grain identity loss" in row["final_reason"]
