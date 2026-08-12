from pathlib import Path


def test_canonical_promotion_workflow_never_deletes_or_saves_the_approved_cache() -> None:
    """The one-lineage contract forbids cache replacement as a promotion mechanism."""
    workflow = (Path(__file__).parents[2] / ".github" / "workflows" / "research_promote_canonical_one_lineage.yml")
    text = workflow.read_text(encoding="utf-8")

    assert "gh api --method DELETE" not in text
    assert "actions/cache/save@" not in text
    assert "Immutable lineage guard" in text
