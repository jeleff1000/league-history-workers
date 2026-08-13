from pathlib import Path


def test_target_inventory_workflow_is_read_only_and_uses_the_approved_cache() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github" / "workflows" / "research_mfl_classification_bridge_target_inventory.yml"
    ).read_text(encoding="utf-8")

    assert "research-public-lake-v4-Linux-20260806-championship-v2-final-playoff-anchor-outcomes-31147899613" in workflow
    assert "actions/cache/restore@v5" in workflow
    assert "materialize_mfl_classification_bridge_targets.py" in workflow
    assert "research_mfl_classification_bridge_targets" in workflow
    assert "actions/cache/save" not in workflow
    assert "cache_mutated" in workflow
    assert "new_lineage" in workflow
