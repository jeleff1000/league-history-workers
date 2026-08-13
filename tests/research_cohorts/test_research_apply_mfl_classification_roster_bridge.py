from pathlib import Path


def test_apply_workflow_is_locked_to_the_validated_1720_row_mfl_classification_delta() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github" / "workflows" / "research_apply_mfl_classification_roster_bridge.yml"
    ).read_text(encoding="utf-8")

    assert "research-mfl-classification-roster-bridge-pilot-${{ inputs.candidate_run_id }}" in workflow
    assert "EXPECTED_DELTA_ROWS: '1720'" in workflow
    assert "EXPECTED_LOSS_FILLS: '1720'" in workflow
    assert "EXPECTED_TIE_FILLS: '1720'" in workflow
    assert "apply_team_signal_delta_in_place.py" in workflow
    assert "'loss':int(con.execute" in workflow
    assert "'tie':int(con.execute" in workflow
    assert "actions/cache/save@v5" in workflow
    assert "extract_mfl_target_roster_memberships.py" not in workflow
    assert "new_lineage" in workflow
