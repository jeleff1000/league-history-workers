from validate_artifact_cache_ledger import summarize_rows, validate_rows
from enrich_artifact_cache_ledger_completion_gates import derive_gate_states


def _row(**overrides):
    with_gates = overrides.pop("_with_gates", True)
    row = {
        "record_type": "artifact_inventory",
        "artifact_id": "1",
        "artifact": "artifact-a",
        "workflow_run_id": "10",
        "canonical_cache_key": "canonical",
        "receipt_run_id": "20",
        "receipt_json_sha256": "abc",
        "final_status": "cache_verified",
        "final_reason": "Restored cache matches every source cell.",
        "next_action": "closed_cache_verified",
        "candidate_delta_rows": "0",
        "cache_missing_cells": "0",
        "cache_conflict_cells": "0",
        "unmatched_cache_cells": "0",
        "blocked_schema_cells": "0",
    }
    row.update(overrides)
    if with_gates:
        row.update(derive_gate_states(row))
    return row


def test_summarize_rows_separates_raw_artifacts_from_cache_recovery_receipts():
    raw_closed = _row(artifact_id="1")
    raw_open = _row(
        artifact_id="2",
        final_status="partial_schema_blocked_unmatched",
        final_reason="Source team identity has no safe player recipient.",
        next_action="reconcile_source_identity_or_close_source_only",
        unmatched_cache_cells="4",
    )
    recovery = _row(
        record_type="cache_recovery_receipt",
        artifact_id="mfl-74-row-recovery-31624673691",
        cache_admission_state="closed_independent_cache_readback",
    )

    assert summarize_rows([raw_closed, raw_open, recovery]) == {
        "ledger_rows": 3,
        "raw_artifact_rows": 2,
        "raw_artifact_closed_rows": 1,
        "raw_artifact_open_rows": 1,
        "cache_recovery_receipt_rows": 1,
        "unknown_record_type_rows": 0,
    }


def test_rejects_cache_verified_row_with_remaining_gap():
    result = validate_rows([_row(cache_missing_cells="1")])

    assert result["ok"] is False
    assert result["issues"] == [
        {
            "artifact_id": "1",
            "issue": "cache_verified_has_nonzero_gap",
        }
    ]


def test_rejects_row_without_explicit_record_type():
    row = _row()
    del row["record_type"]

    result = validate_rows([row])

    assert result["ok"] is False
    assert result["issues"] == [
        {"artifact_id": "1", "issue": "missing_record_type"},
    ]


def test_allows_explicit_null_nfl_player_candidate_closure():
    result = validate_rows([
        _row(
            final_status="no_promotable_candidate_emitted",
            final_reason=(
                "Strict candidate audit found no valid NFL-player fill; "
                "the only combined NULL fill has NFL_player_id null."
            ),
            next_action="closed_no_promotable_candidate_emitted",
            candidate_delta_rows="165",
            cache_missing_cells="1",
        )
    ])

    assert result["ok"] is True
    assert result["issues"] == []


def test_rejects_open_row_without_a_receipt():
    result = validate_rows([
        _row(
            final_status="partial_schema_blocked_unmatched",
            final_reason="Source team identity has no safe player recipient.",
            next_action="reconcile_source_identity_or_close_source_only",
            receipt_run_id="",
            receipt_json_sha256="",
            unmatched_cache_cells="4",
        )
    ])

    assert result["ok"] is False
    assert result["issues"] == [
        {"artifact_id": "1", "issue": "missing_receipt_run_id"},
        {"artifact_id": "1", "issue": "missing_receipt_json_sha256"},
    ]


def test_rejects_incomplete_player_team_bridge_evidence():
    result = validate_rows([
        _row(
            final_status="partial_schema_blocked_unmatched",
            final_reason="Source team identity has no safe player recipient.",
            next_action="reconcile_source_identity_or_close_source_only",
            unmatched_cache_cells="4",
            bridge_evidence_file_count="1",
            bridge_evidence_row_count="0",
            bridge_evidence_strength="",
            bridge_evidence_receipt_run_id="",
        )
    ])

    assert result["ok"] is False
    assert result["issues"] == [
        {"artifact_id": "1", "issue": "bridge_evidence_has_no_rows"},
        {"artifact_id": "1", "issue": "bridge_evidence_missing_strength"},
        {"artifact_id": "1", "issue": "bridge_evidence_missing_receipt"},
    ]


def test_rejects_conflict_without_precedence_action():
    result = validate_rows([
        _row(
            final_status="cache_conflict_preserved",
            final_reason="Source conflicts with a canonical non-null value.",
            next_action="preserve_conflicts",
            cache_conflict_cells="1",
        )
    ])

    assert result["ok"] is False
    assert result["issues"] == [
        {"artifact_id": "1", "issue": "conflict_missing_precedence_action"},
    ]


def test_rejects_open_row_without_machine_gate_states():
    result = validate_rows([
        _row(
            _with_gates=False,
            final_status="partial_schema_blocked_unmatched",
            final_reason="Source team identity has no safe player recipient.",
            next_action="reconcile_source_identity_or_close_source_only",
            unmatched_cache_cells="4",
        )
    ])

    assert result["ok"] is False
    assert result["issues"] == [
        {"artifact_id": "1", "issue": "missing_loss_tie_gate"},
        {"artifact_id": "1", "issue": "missing_player_team_bridge_gate"},
        {"artifact_id": "1", "issue": "missing_source_precedence_gate"},
        {"artifact_id": "1", "issue": "missing_cache_admission_state"},
    ]


def test_rejects_gate_state_that_claims_bridge_is_not_required():
    result = validate_rows([
        _row(
            _with_gates=False,
            final_status="partial_schema_blocked_unmatched",
            final_reason="Source team identity has no safe player recipient.",
            next_action="reconcile_source_identity_or_close_source_only",
            unmatched_cache_cells="4",
            loss_tie_gate="not_required",
            player_team_bridge_gate="not_required",
            source_precedence_gate="not_required",
            cache_admission_state="open_player_team_bridge",
        )
    ])

    assert result["ok"] is False
    assert result["issues"] == [
        {"artifact_id": "1", "issue": "incorrect_player_team_bridge_gate"},
    ]
