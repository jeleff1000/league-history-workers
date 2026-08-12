import csv
import json

from enrich_artifact_cache_ledger_bridge_evidence import enrich


def test_attaches_player_team_bridge_receipt_to_existing_ledger_rows(tmp_path):
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["artifact_id", "artifact"])
        writer.writeheader()
        writer.writerows([
            {"artifact_id": "1", "artifact": "candidate-team"},
            {"artifact_id": "2", "artifact": "manager-only"},
            {"artifact_id": "3", "artifact": "not-a-bridge"},
        ])

    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"files": [
        {
            "artifact_id": 1,
            "file": "players.parquet",
            "rows": 12,
            "columns": ["db_name", "year", "week", "NFL_player_id", "manager", "team_name"],
        },
        {
            "artifact_id": 2,
            "file": "manager_players.parquet",
            "rows": 4,
            "columns": ["db_name", "year", "week", "NFL_player_id", "manager"],
        },
        {
            "artifact_id": 3,
            "file": "empty_players.parquet",
            "rows": 0,
            "columns": ["db_name", "year", "week", "NFL_player_id", "manager", "team_name"],
        },
    ]}), encoding="utf-8")

    out = tmp_path / "out.csv"
    result = enrich(
        ledger_path=ledger,
        receipt_path=receipt,
        receipt_run_id="31448116124",
        out_path=out,
    )

    assert result == {
        "ledger_rows": 3,
        "bridge_artifacts": 2,
        "player_team_candidate_artifacts": 1,
        "manager_only_candidate_artifacts": 1,
        "unknown_receipt_artifacts": 0,
    }
    rows = {row["artifact_id"]: row for row in csv.DictReader(out.open(newline="", encoding="utf-8"))}
    assert rows["1"]["bridge_evidence_file_count"] == "1"
    assert rows["1"]["bridge_evidence_row_count"] == "12"
    assert rows["1"]["bridge_evidence_strength"] == "candidate_player_manager_team_name"
    assert rows["1"]["bridge_evidence_receipt_run_id"] == "31448116124"
    assert rows["2"]["bridge_evidence_strength"] == "candidate_player_manager_only"
    assert rows["3"]["bridge_evidence_file_count"] == ""


def test_excludes_derived_player_delta_from_independent_bridge_evidence(tmp_path):
    """A cache-derived delta cannot prove the source player-to-team edge."""
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["artifact_id", "artifact"])
        writer.writeheader()
        writer.writerow({"artifact_id": "1", "artifact": "derived-delta"})

    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"files": [{
        "artifact_id": 1,
        "file": "promotable_delta.parquet",
        "rows": 12,
        "columns": ["db_name", "year", "week", "NFL_player_id", "manager", "team_key"],
    }]}), encoding="utf-8")

    out = tmp_path / "out.csv"
    result = enrich(
        ledger_path=ledger,
        receipt_path=receipt,
        receipt_run_id="31500000000",
        out_path=out,
    )

    assert result["bridge_artifacts"] == 0
    row = next(csv.DictReader(out.open(newline="", encoding="utf-8")))
    assert row["bridge_evidence_strength"] == ""
