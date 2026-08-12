import csv

from select_retained_artifacts import select


def test_selection_ignores_supplemental_cache_recovery_receipts(tmp_path):
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["record_type", "artifact_id", "artifact", "final_status"])
        writer.writeheader()
        writer.writerows([
            {"record_type": "artifact_inventory", "artifact_id": "1", "artifact": "team-a", "final_status": "partial_schema_blocked_unmatched"},
            {"record_type": "cache_recovery_receipt", "artifact_id": "mfl-74-row-recovery-31624673691", "artifact": "receipt", "final_status": "partial_schema_blocked_unmatched"},
        ])

    assert [row["artifact_id"] for row in select(
        ledger, source_kind="team", prefix="", requested_ids=set()
    )] == ["1"]


def test_team_selection_rechecks_legacy_loss_tie_schema_blocked_artifacts(tmp_path):
    ledger = tmp_path / "ledger.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["record_type", "artifact_id", "artifact", "final_status"])
        writer.writeheader()
        writer.writerows([
            {"record_type": "artifact_inventory", "artifact_id": "1", "artifact": "team-a", "final_status": "partial_schema_blocked_direct_identity"},
            {"record_type": "artifact_inventory", "artifact_id": "2", "artifact": "team-b", "final_status": "partial_schema_blocked_conflicts"},
        ])

    assert [row["artifact_id"] for row in select(
        ledger, source_kind="team", prefix="", requested_ids=set()
    )] == ["1", "2"]
