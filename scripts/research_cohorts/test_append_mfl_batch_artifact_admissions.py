import csv
from pathlib import Path

import pytest

from append_mfl_batch_artifact_admissions import append_admissions
from validate_artifact_cache_ledger import validate_rows


FIELDS = [
    "record_type", "receipt_run_id", "canonical_cache_key", "receipt_json_sha256",
    "artifact_id", "artifact", "workflow_run_id", "dispositions",
    "candidate_delta_rows", "candidate_fields", "candidate_file_count", "candidate_files",
    "source_cells", "cache_match_cells", "cache_missing_cells", "cache_conflict_cells",
    "unmatched_cache_cells", "blocked_schema_cells", "final_status", "final_reason",
    "next_action", "identity_profile_run_id", "identity_profile_source", "source_team_keys",
    "matched_source_team_keys", "unmatched_source_team_keys", "league_week_overlap_keys",
    "league_week_absent_keys", "matched_source_player_rows", "identity_bridge_status",
    "matched_by_mfl_manager_guid", "matched_by_manager_team_name", "matched_by_unique_manager",
    "cache_join_strategy", "bridge_evidence_file_count", "bridge_evidence_row_count",
    "bridge_evidence_strength", "bridge_evidence_schemas", "bridge_evidence_receipt_run_id",
    "loss_tie_gate", "player_team_bridge_gate", "source_precedence_gate", "cache_admission_state",
]


def _ledger(path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerow({
            "record_type": "artifact_inventory", "receipt_run_id": "source", "canonical_cache_key": "cache",
            "receipt_json_sha256": "sha", "artifact_id": "old", "artifact": "old-artifact",
            "workflow_run_id": "old-run", "final_status": "not_data_bearing",
            "final_reason": "no candidate data", "next_action": "closed", "cache_admission_state": "closed_not_data_bearing",
        })


def _manifest(path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "artifact_index", "artifact", "artifact_id", "admission_state", "reason",
            "candidate_rows", "win_cells", "loss_cells", "tie_cells", "team_points_cells", "playoff_cells",
            "receipt_run_id", "source_run_id",
        ])
        writer.writeheader()
        writer.writerows([
            {"artifact_index": "0", "artifact": "candidate", "artifact_id": "1", "admission_state": "included_in_verified_cache_batch", "reason": "included", "candidate_rows": "2", "win_cells": "1", "loss_cells": "2", "tie_cells": "2", "team_points_cells": "2", "playoff_cells": "2", "receipt_run_id": "readback", "source_run_id": "source"},
            {"artifact_index": "1", "artifact": "zero", "artifact_id": "2", "admission_state": "admitted_no_promotable_delta", "reason": "zero", "candidate_rows": "0", "win_cells": "0", "loss_cells": "0", "tie_cells": "0", "team_points_cells": "0", "playoff_cells": "0", "receipt_run_id": "readback", "source_run_id": "source"},
            {"artifact_index": "2", "artifact": "diagnostic", "artifact_id": "3", "admission_state": "source_diagnostic_only", "reason": "diagnostic", "candidate_rows": "0", "win_cells": "0", "loss_cells": "0", "tie_cells": "0", "team_points_cells": "0", "playoff_cells": "0", "receipt_run_id": "readback", "source_run_id": "source"},
            {"artifact_index": "3", "artifact": "excluded", "artifact_id": "4", "admission_state": "explicitly_excluded_no_candidate", "reason": "excluded", "candidate_rows": "0", "win_cells": "0", "loss_cells": "0", "tie_cells": "0", "team_points_cells": "0", "playoff_cells": "0", "receipt_run_id": "readback", "source_run_id": "source"},
        ])


def test_appends_every_mfl_batch_artifact_without_rewriting_frozen_inventory(tmp_path: Path) -> None:
    ledger, manifest, out = tmp_path / "ledger.csv", tmp_path / "manifest.csv", tmp_path / "out.csv"
    _ledger(ledger)
    _manifest(manifest)

    result = append_admissions(ledger, manifest, out, canonical_cache_key="cache", receipt_sha256="sha")

    assert result == {"ledger_rows": 5, "artifact_admission_rows": 4}
    with out.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["record_type"] == "artifact_inventory"
    assert [r["final_status"] for r in rows[1:]] == [
        "cache_verified", "no_promotable_candidate_emitted", "not_data_bearing", "not_data_bearing",
    ]
    assert validate_rows(rows)["ok"]


def test_rejects_artifact_already_present_in_ledger(tmp_path: Path) -> None:
    ledger, manifest, out = tmp_path / "ledger.csv", tmp_path / "manifest.csv", tmp_path / "out.csv"
    _ledger(ledger)
    _manifest(manifest)
    with ledger.open("a", newline="", encoding="utf-8") as handle:
        csv.DictWriter(handle, fieldnames=FIELDS).writerow({"artifact_id": "1", "artifact": "candidate"})

    with pytest.raises(ValueError, match="already exists"):
        append_admissions(ledger, manifest, out, canonical_cache_key="cache", receipt_sha256="sha")


def test_uses_an_explicit_non_github_receipt_id_when_an_excluded_shard_emitted_no_artifact(tmp_path: Path) -> None:
    ledger, manifest, out = tmp_path / "ledger.csv", tmp_path / "manifest.csv", tmp_path / "out.csv"
    _ledger(ledger)
    _manifest(manifest)
    rows = list(csv.DictReader(manifest.open(newline="", encoding="utf-8")))
    rows[-1]["artifact_id"] = ""
    rows[-1]["artifact_index"] = "3"
    with manifest.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    append_admissions(ledger, manifest, out, canonical_cache_key="cache", receipt_sha256="sha")

    with out.open(newline="", encoding="utf-8") as handle:
        appended = list(csv.DictReader(handle))[-1]
    assert appended["artifact_id"] == "no-github-artifact:run-source:shard-3"
    assert appended["final_reason"] == "excluded"
