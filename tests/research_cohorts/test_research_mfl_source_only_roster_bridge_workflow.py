from pathlib import Path


def test_full_mfl_source_only_workflow_uses_the_proven_direct_team_player_bridge() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github" / "workflows" / "research_mfl_source_only_roster_bridge.yml"
    ).read_text(encoding="utf-8")

    assert "build_mfl_source_team_player_bridge.py" in workflow
    assert "--team-signals shard_source_signals.parquet" in workflow
    assert "build_mfl_roster_bridge_receipt.py" not in workflow
    assert "actions/cache/save" not in workflow
    assert "gh api --method DELETE" not in workflow
