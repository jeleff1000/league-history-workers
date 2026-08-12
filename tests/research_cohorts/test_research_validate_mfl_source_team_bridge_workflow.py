from pathlib import Path


def test_validation_workflow_uses_captured_artifact_and_never_saves_a_cache() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github" / "workflows" / "research_validate_mfl_source_team_bridge.yml"
    ).read_text(encoding="utf-8")

    assert "mfl-source-only-roster-bridge-${SHARD}-${SOURCE_RUN_ID}" in workflow
    assert "build_mfl_source_team_player_bridge.py" in workflow
    assert "audit_team_signal_candidates.py" in workflow
    assert "actions/cache/save" not in workflow
    assert "extract_mfl_target_roster_memberships.py" not in workflow
