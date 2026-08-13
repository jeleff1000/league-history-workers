from pathlib import Path


def test_exact_mfl_duplicate_promotion_workflow_is_same_lineage_and_readback_guarded():
    workflow = Path(".github/workflows/research_apply_exact_mfl_duplicate_bridge_candidate.yml").read_text(
        encoding="utf-8"
    )

    assert "research-public-lake-v4-Linux-20260806-championship-v2-final-playoff-anchor-outcomes-31147899613" in workflow
    assert "actions/cache/restore@v5" in workflow
    assert "actions/cache/save@v5" in workflow
    assert "apply_exact_mfl_duplicate_bridge_candidate.py" in workflow
    assert "candidate/cache base hash mismatch" in workflow
    assert "ops cache changed" in workflow
    assert "fresh_restore" in workflow
    assert "new_lineage" in workflow
