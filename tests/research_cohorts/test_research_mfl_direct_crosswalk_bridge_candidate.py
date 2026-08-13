from __future__ import annotations

from pathlib import Path


def test_direct_crosswalk_bridge_candidate_is_read_only_and_fixed_lineage() -> None:
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_mfl_direct_crosswalk_bridge_candidate.yml"
    ).read_text(encoding="utf-8")

    assert "research-public-lake-v4-Linux-20260806-championship-v2-final-playoff-anchor-outcomes-31147899613" in workflow
    assert "DIRECT_CROSSWALK_RUN_ID: \"31736789016\"" in workflow
    assert "SOURCE_ROSTER_RUN_ID: \"31663728938\"" in workflow
    assert "fail-on-cache-miss: true" in workflow
    assert "build_mfl_source_team_player_bridge.py" in workflow
    assert "audit_team_signal_candidates.py" in workflow
    assert "sha256sum research_public_lake/corpus_snapshot.duckdb" in workflow
    assert "cmp -s cache_before.sha256 cache_after.sha256" in workflow
    assert "cmp -s ops_before.sha256 ops_after.sha256" in workflow
    assert "actions/cache/save" not in workflow
