import csv
from pathlib import Path


def test_refresh_preserves_historical_counts_but_removes_stale_schema_action(tmp_path: Path) -> None:
    from refresh_stale_loss_tie_ledger_language import refresh

    ledger = tmp_path / "ledger.csv"
    fields = [
        "record_type", "artifact_id", "final_status", "final_reason", "next_action",
        "blocked_schema_cells", "unmatched_cache_cells", "cache_conflict_cells",
        "loss_tie_gate", "player_team_bridge_gate", "source_precedence_gate",
        "cache_admission_state",
    ]
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({
            "record_type": "artifact_inventory", "artifact_id": "1",
            "final_status": "partial_schema_blocked_conflicts",
            "final_reason": "Source has non-null fields absent from schema.",
            "next_action": "add_loss_tie_schema_then_resolve_direct_identity_and_precedence_adjudication",
            "blocked_schema_cells": "3", "unmatched_cache_cells": "2",
            "cache_conflict_cells": "1",
        })
    out = tmp_path / "out.csv"
    assert refresh(ledger_path=ledger, out_path=out) == {
        "ledger_rows": 1, "stale_rows_refreshed": 1,
    }
    row = list(csv.DictReader(out.open(newline="", encoding="utf-8")))[0]
    assert row["blocked_schema_cells"] == "3"
    assert row["loss_tie_gate"] == "closed_schema_supported"
    assert row["next_action"] == "resolve_direct_identity_and_precedence_adjudication"
    assert "Current canonical schema supports loss/tie" in row["final_reason"]
    assert row["cache_admission_state"] == "open_player_team_bridge_and_source_precedence"
