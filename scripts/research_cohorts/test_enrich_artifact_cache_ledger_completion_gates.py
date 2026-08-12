import csv

from enrich_artifact_cache_ledger_completion_gates import derive_gate_states, enrich


def _row(**overrides):
    row = {
        "final_status": "partial_schema_blocked_conflicts",
        "next_action": "add_loss_tie_schema_then_resolve_direct_identity_and_precedence_adjudication",
        "blocked_schema_cells": "2",
        "unmatched_cache_cells": "3",
        "cache_conflict_cells": "4",
    }
    row.update(overrides)
    return row


def test_marks_all_three_open_gates_from_quantified_evidence():
    assert derive_gate_states(_row()) == {
        "loss_tie_gate": "open_schema_required",
        "player_team_bridge_gate": "open_raw_roster_bridge_required",
        "source_precedence_gate": "open_field_precedence_required",
        "cache_admission_state": "open_all_required_gates",
    }


def test_marks_verified_artifact_as_closed_for_all_gates():
    assert derive_gate_states(_row(
        final_status="cache_verified",
        next_action="closed_cache_verified",
        blocked_schema_cells="0",
        unmatched_cache_cells="0",
        cache_conflict_cells="0",
    )) == {
        "loss_tie_gate": "not_required",
        "player_team_bridge_gate": "not_required",
        "source_precedence_gate": "not_required",
        "cache_admission_state": "closed_cache_verified",
    }


def test_does_not_treat_manager_only_action_as_completed_bridge():
    result = derive_gate_states(_row(
        final_status="partial_schema_blocked_unmatched",
        next_action="reconcile_source_identity_or_close_source_only",
        blocked_schema_cells="0",
        unmatched_cache_cells="1",
        cache_conflict_cells="0",
    ))

    assert result["player_team_bridge_gate"] == "open_raw_roster_bridge_required"
    assert result["cache_admission_state"] == "open_player_team_bridge"


def test_enrich_writes_gate_columns_without_losing_existing_columns(tmp_path):
    ledger = tmp_path / "ledger.csv"
    out = tmp_path / "out.csv"
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "artifact_id", "final_status", "next_action", "blocked_schema_cells",
            "unmatched_cache_cells", "cache_conflict_cells", "custom_receipt_field",
        ])
        writer.writeheader()
        writer.writerow({
            "artifact_id": "1",
            "final_status": "partial_schema_blocked_unmatched",
            "next_action": "reconcile_source_identity_or_close_source_only",
            "blocked_schema_cells": "0",
            "unmatched_cache_cells": "4",
            "cache_conflict_cells": "0",
            "custom_receipt_field": "preserved",
        })

    assert enrich(ledger_path=ledger, out_path=out) == {
        "ledger_rows": 1,
        "raw_artifact_rows": 1,
        "cache_recovery_receipt_rows": 0,
        "closed_rows": 0,
        "open_rows": 1,
    }
    with out.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == [{
        "artifact_id": "1",
        "record_type": "artifact_inventory",
        "final_status": "partial_schema_blocked_unmatched",
        "next_action": "reconcile_source_identity_or_close_source_only",
        "blocked_schema_cells": "0",
        "unmatched_cache_cells": "4",
        "cache_conflict_cells": "0",
        "custom_receipt_field": "preserved",
        "loss_tie_gate": "not_required",
        "player_team_bridge_gate": "open_raw_roster_bridge_required",
        "source_precedence_gate": "not_required",
        "cache_admission_state": "open_player_team_bridge",
    }]


def test_enrich_adds_explicit_record_types_and_keeps_supplemental_receipts_out_of_artifact_totals(tmp_path):
    ledger = tmp_path / "ledger.csv"
    out = tmp_path / "out.csv"
    fields = [
        "artifact_id", "final_status", "next_action", "blocked_schema_cells",
        "unmatched_cache_cells", "cache_conflict_cells",
    ]
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({
            "artifact_id": "1",
            "final_status": "cache_verified",
            "next_action": "closed_cache_verified",
            "blocked_schema_cells": "0",
            "unmatched_cache_cells": "0",
            "cache_conflict_cells": "0",
        })
        writer.writerow({
            "artifact_id": "mfl-74-row-recovery-31624673691",
            "final_status": "cache_verified",
            "next_action": "closed_independent_cache_readback",
            "blocked_schema_cells": "0",
            "unmatched_cache_cells": "0",
            "cache_conflict_cells": "0",
        })

    assert enrich(ledger_path=ledger, out_path=out) == {
        "ledger_rows": 2,
        "raw_artifact_rows": 1,
        "cache_recovery_receipt_rows": 1,
        "closed_rows": 1,
        "open_rows": 0,
    }
    with out.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["record_type"] for row in rows] == [
        "artifact_inventory",
        "cache_recovery_receipt",
    ]
