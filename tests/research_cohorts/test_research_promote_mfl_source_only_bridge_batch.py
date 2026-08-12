from pathlib import Path


def test_batch_promotion_workflow_requires_only_safe_read_only_mfl_candidates() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github"
        / "workflows"
        / "research_promote_mfl_source_only_bridge_batch.yml"
    ).read_text(encoding="utf-8")

    assert "research-public-lake-v4-Linux-20260806-championship-v2-final-playoff-anchor-outcomes-31147899613" in workflow
    assert "apply_team_signal_delta_in_place.py" in workflow
    assert "mfl-source-only-roster-bridge-" in workflow
    assert "cache_mutated" in workflow
    assert "canonical_schema_unchanged" in workflow
    assert "positive_mismatches_not_auto_promoted" in workflow
    assert "inserted_rows" in workflow
    assert "actions/cache/save@v5" in workflow
    assert "Prove exactly one same-key cache object remains" in workflow
    assert "restore-keys:" not in workflow
