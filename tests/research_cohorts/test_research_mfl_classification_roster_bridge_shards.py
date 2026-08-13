from pathlib import Path


def test_mfl_classification_shards_are_read_only_and_bounded() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github" / "workflows" / "research_mfl_classification_roster_bridge_shards.yml"
    ).read_text(encoding="utf-8")

    assert "MAX_SHARDS = 256" in workflow
    assert "max-parallel: 15" in workflow
    assert "extract_mfl_target_roster_memberships.py" in workflow
    assert "--shard ${{ matrix.shard }} --shards ${{ matrix.shards }}" in workflow
    assert "audit_team_signal_candidates.py" in workflow
    assert "actions/cache/save@v5" not in workflow
    assert "actions/caches/" not in workflow
    assert "cache_mutated" in workflow
    assert "new_lineage" in workflow
