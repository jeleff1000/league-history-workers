import csv
import json
from pathlib import Path


def _write_ledger(path: Path) -> None:
    fields = ["artifact_id", "final_status", "final_reason", "next_action"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows([
            {"artifact_id": "1", "final_status": "partial_schema_blocked_unmatched",
             "final_reason": "open", "next_action": "reconcile"},
            {"artifact_id": "2", "final_status": "cache_verified",
             "final_reason": "closed", "next_action": "closed"},
        ])


def test_enrichment_uses_latest_profile_and_preserves_existing_ledger_fields(tmp_path: Path) -> None:
    from scripts.research_cohorts.enrich_artifact_cache_ledger_identity import enrich

    ledger = tmp_path / "ledger.csv"
    _write_ledger(ledger)
    old_profile = tmp_path / "receipt_31510000000" / "raw_signal_join_key_profile.json"
    new_profile = tmp_path / "receipt_31520000000" / "raw_signal_join_key_profile.json"
    old_profile.parent.mkdir()
    new_profile.parent.mkdir()
    old_profile.write_text(json.dumps({"rows": [{
        "artifact_id": 1, "source_team_keys": 5, "matched_source_team_keys": 1,
        "unmatched_source_team_keys": 4, "league_week_overlap_keys": 5,
        "league_week_absent_keys": 0, "matched_source_player_rows": 10,
    }]}))
    new_profile.write_text(json.dumps({"rows": [{
        "artifact_id": 1, "source_team_keys": 5, "matched_source_team_keys": 5,
        "unmatched_source_team_keys": 0, "league_week_overlap_keys": 5,
        "league_week_absent_keys": 0, "matched_source_player_rows": 50,
        "matched_by_unique_manager": 5,
    }]}))
    out = tmp_path / "out.csv"

    result = enrich(ledger_path=ledger, profile_paths=[old_profile, new_profile], out_path=out)

    assert result == {"ledger_rows": 2, "profiled_artifacts": 1, "unmatched_profile_artifacts": 0}
    rows = {row["artifact_id"]: row for row in csv.DictReader(out.open(newline="", encoding="utf-8"))}
    assert rows["1"]["final_status"] == "partial_schema_blocked_unmatched"
    assert rows["1"]["identity_profile_run_id"] == "31520000000"
    assert rows["1"]["matched_source_team_keys"] == "5"
    assert rows["1"]["unmatched_source_team_keys"] == "0"
    assert rows["1"]["matched_by_unique_manager"] == "5"
    assert rows["1"]["cache_join_strategy"] == "manager_week_fanout"
    assert rows["1"]["identity_bridge_status"] == "fully_matched"
    assert rows["2"]["identity_profile_run_id"] == ""
    assert b"\r\n" not in out.read_bytes()
