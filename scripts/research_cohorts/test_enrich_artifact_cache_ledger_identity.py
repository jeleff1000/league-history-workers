import csv
import json

from enrich_artifact_cache_ledger_identity import enrich


def test_identity_enrichment_profiles_only_raw_artifact_records(tmp_path):
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["record_type", "artifact_id", "artifact"])
        writer.writeheader()
        writer.writerows([
            {"record_type": "artifact_inventory", "artifact_id": "1", "artifact": "candidate"},
            {
                "record_type": "cache_recovery_receipt",
                "artifact_id": "mfl-74-row-recovery-31624673691",
                "artifact": "receipt",
            },
        ])
    profile = tmp_path / "profile-31600000000.json"
    profile.write_text(json.dumps({"rows": [{
        "artifact_id": 1,
        "source_team_keys": 2,
        "matched_source_team_keys": 2,
        "unmatched_source_team_keys": 0,
        "league_week_overlap_keys": 2,
        "league_week_absent_keys": 0,
        "matched_source_player_rows": 20,
        "matched_by_unique_manager": 2,
    }]}), encoding="utf-8")
    out = tmp_path / "out.csv"

    assert enrich(ledger_path=ledger, profile_paths=[profile], out_path=out) == {
        "ledger_rows": 2,
        "profiled_artifacts": 1,
        "unmatched_profile_artifacts": 0,
    }
    rows = list(csv.DictReader(out.open(newline="", encoding="utf-8")))
    assert rows[0]["identity_bridge_status"] == "fully_matched"
    assert rows[1]["artifact_id"] == "mfl-74-row-recovery-31624673691"
    assert rows[1]["identity_bridge_status"] == ""
