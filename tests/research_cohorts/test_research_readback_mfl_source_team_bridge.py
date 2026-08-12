from pathlib import Path


def test_readback_workflow_is_restore_only_and_uses_literal_duckdb_path() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github" / "workflows" / "research_readback_mfl_source_team_bridge.yml"
    ).read_text(encoding="utf-8")

    assert "mfl-source-team-bridge-apply-evidence-${{ inputs.apply_run_id }}" in workflow
    assert "mfl-source-team-bridge-readback" in workflow
    assert "actions/cache/save" not in workflow
    assert "gh api --method DELETE" not in workflow
    assert "CREATE TEMP VIEW d AS SELECT * FROM read_parquet(?)" not in workflow
    assert "team_points_remaining_null_source_cells" in workflow
