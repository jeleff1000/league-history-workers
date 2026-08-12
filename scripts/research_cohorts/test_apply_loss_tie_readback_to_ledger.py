import csv
import json

import pytest

from apply_loss_tie_readback_to_ledger import reconcile


def _row(artifact_id: str) -> dict[str, str]:
    return {
        "record_type": "artifact_inventory",
        "artifact_id": artifact_id,
        "artifact": f"artifact-{artifact_id}",
        "workflow_run_id": "source-run",
        "canonical_cache_key": "approved-key",
        "receipt_run_id": "old-run",
        "receipt_json_sha256": "old-hash",
        "candidate_delta_rows": "1",
        "source_cells": "10",
        "cache_match_cells": "8",
        "cache_missing_cells": "0",
        "cache_conflict_cells": "0",
        "unmatched_cache_cells": "0",
        "blocked_schema_cells": "2",
        "final_status": "blocked_schema",
        "final_reason": "old",
        "next_action": "old",
        "loss_tie_gate": "open_schema_required",
        "player_team_bridge_gate": "not_required",
        "source_precedence_gate": "not_required",
        "cache_admission_state": "open_loss_tie",
    }


def _write_ledger(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_reconcile_closes_exact_artifacts_only_after_clean_fresh_readback(tmp_path):
    ledger = tmp_path / "ledger.csv"
    receipt = tmp_path / "readback.json"
    output = tmp_path / "out.csv"
    _write_ledger(ledger, [_row("1"), _row("2"), _row("3")])
    receipt.write_text(json.dumps({
        "canonical_cache_key": "approved-key",
        "loss_remaining_null_source_cells": 0,
        "tie_remaining_null_source_cells": 0,
        "fresh_restore": True,
        "new_lineage": False,
    }))

    result = reconcile(
        ledger_path=ledger,
        readback_path=receipt,
        artifact_ids={"1", "2"},
        receipt_run_id="verified-run",
        out_path=output,
    )

    assert result == {"closed_artifacts": 2}
    with output.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["final_status"] == "cache_verified"
    assert rows[0]["cache_match_cells"] == "10"
    assert rows[0]["blocked_schema_cells"] == "0"
    assert rows[0]["cache_admission_state"] == "closed_cache_verified"
    assert rows[2]["final_status"] == "blocked_schema"


def test_reconcile_refuses_readback_with_remaining_source_nulls(tmp_path):
    ledger = tmp_path / "ledger.csv"
    receipt = tmp_path / "readback.json"
    output = tmp_path / "out.csv"
    _write_ledger(ledger, [_row("1")])
    receipt.write_text(json.dumps({
        "canonical_cache_key": "approved-key",
        "loss_remaining_null_source_cells": 1,
        "tie_remaining_null_source_cells": 0,
        "fresh_restore": True,
        "new_lineage": False,
    }))

    with pytest.raises(SystemExit, match="remaining source-null"):
        reconcile(
            ledger_path=ledger,
            readback_path=receipt,
            artifact_ids={"1"},
            receipt_run_id="verified-run",
            out_path=output,
        )
