import csv
import json

from apply_partial_team_candidate_readback_to_ledger import reconcile


def _row() -> dict[str, str]:
    return {
        "record_type": "artifact_inventory",
        "artifact_id": "8952555354",
        "canonical_cache_key": "approved-key",
        "receipt_run_id": "old-run",
        "receipt_json_sha256": "old-sha",
        "source_cells": "182569",
        "cache_match_cells": "158001",
        "cache_missing_cells": "0",
        "cache_conflict_cells": "0",
        "unmatched_cache_cells": "11736",
        "blocked_schema_cells": "12832",
        "final_status": "partial_schema_blocked_unmatched",
        "final_reason": "old",
        "next_action": "old",
        "loss_tie_gate": "open_schema_required",
        "player_team_bridge_gate": "open_raw_roster_bridge_required",
        "source_precedence_gate": "not_required",
        "cache_admission_state": "open_loss_tie_and_player_team_bridge",
        "matched_source_player_rows": "52667",
        "identity_profile_run_id": "old-profile",
        "identity_profile_source": "old-profile.json",
    }


def test_reconcile_records_partial_cache_promotion_without_closing_source_only_keys(tmp_path):
    ledger = tmp_path / "ledger.csv"
    promotion = tmp_path / "promotion.json"
    refreshed = tmp_path / "refreshed.json"
    output = tmp_path / "out.csv"
    row = _row()
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    promotion.write_text(json.dumps({
        "canonical_cache_key": "approved-key",
        "source_artifact_id": 8952555354,
        "candidate_rows": 52667,
        "matched_player_rows": 52667,
        "unmatched_candidate_keys": 0,
        "loss_remaining_null_source_cells": 0,
        "tie_remaining_null_source_cells": 0,
        "schema_unchanged": True,
        "player_rows_unchanged": True,
        "ops_unchanged": True,
        "fresh_restore": True,
        "new_lineage": False,
    }))
    refreshed.write_text(json.dumps({"rows": [{
        "artifact_id": "8952555354",
        "source_cells": 282895,
        "cache_match_cells": 263335,
        "cache_missing_cells": 0,
        "cache_conflict_cells": 0,
        "unmatched_cache_cells": 19560,
        "blocked_schema_cells": 0,
        "final_status": "unmatched_cache_key",
        "final_reason": "Source candidate key has no matching current canonical row.",
    }]}))

    result = reconcile(
        ledger_path=ledger,
        promotion_readback_path=promotion,
        refreshed_receipts_path=refreshed,
        receipt_run_id="promotion-run",
        refreshed_readback_run_id="readback-run",
        out_path=output,
    )

    assert result == {"updated_artifacts": 1}
    with output.open(newline="", encoding="utf-8") as handle:
        updated = next(csv.DictReader(handle))
    assert updated["receipt_run_id"] == "promotion-run"
    assert updated["cache_match_cells"] == "263335"
    assert updated["blocked_schema_cells"] == "0"
    assert updated["final_status"] == "unmatched_cache_key"
    assert updated["loss_tie_gate"] == "not_required"
    assert updated["player_team_bridge_gate"] == "open_raw_roster_bridge_required"
    assert updated["cache_admission_state"] == "open_player_team_bridge"
