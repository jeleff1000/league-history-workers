from validate_artifact_cache_ledger import validate_rows


def _row(**overrides):
    row = {
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
    return row


def test_rejects_cache_verified_row_with_remaining_gap():
    result = validate_rows([_row(cache_missing_cells="1")])

    assert result["ok"] is False
    assert result["issues"] == [
        {
            "artifact_id": "1",
            "issue": "cache_verified_has_nonzero_gap",
        }
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
