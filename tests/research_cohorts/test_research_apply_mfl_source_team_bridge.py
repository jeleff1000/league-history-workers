from pathlib import Path


def test_apply_workflow_is_locked_to_the_validated_204_row_null_fill() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github" / "workflows" / "research_apply_mfl_source_team_bridge.yml"
    ).read_text(encoding="utf-8")

    assert "mfl-source-team-bridge-validation-4-${{ inputs.candidate_run_id }}" in workflow
    assert "EXPECTED_DELTA_ROWS: '204'" in workflow
    assert "apply_team_signal_delta_in_place.py" in workflow
    assert "loss_remaining_null_source_cells" in workflow
    assert "team_points_remaining_null_source_cells" in workflow
    assert "playoffs_remaining_null_source_cells" in workflow
    assert "actions/cache/save@v5" in workflow
    assert "extract_mfl_target_roster_memberships.py" not in workflow
    assert "CREATE TEMP VIEW d AS SELECT * FROM read_parquet(?)" not in workflow
