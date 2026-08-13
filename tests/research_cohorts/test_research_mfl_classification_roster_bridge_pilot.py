from pathlib import Path


def test_roster_bridge_pilot_is_read_only_and_limits_itself_to_15_current_targets() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github" / "workflows" / "research_mfl_classification_roster_bridge_pilot.yml"
    ).read_text(encoding="utf-8")

    assert "research-public-lake-v4-Linux-20260806-championship-v2-final-playoff-anchor-outcomes-31147899613" in workflow
    assert "actions/cache/restore@v5" in workflow
    assert "actions/cache/save" not in workflow
    assert "max-groups 15" in workflow
    assert "extract_mfl_target_roster_memberships.py" in workflow
    assert "build_mfl_classification_team_signals.py" in workflow
    assert "build_mfl_source_team_player_bridge.py" in workflow
    assert "audit_team_signal_candidates.py" in workflow
    assert "cache_mutated" in workflow
    assert "new_lineage" in workflow
