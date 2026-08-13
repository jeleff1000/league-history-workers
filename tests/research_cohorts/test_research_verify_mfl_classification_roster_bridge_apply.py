from pathlib import Path


def test_mfl_apply_verifier_is_read_only_and_uses_saved_apply_evidence() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github"
        / "workflows"
        / "research_verify_mfl_classification_roster_bridge_apply.yml"
    ).read_text(encoding="utf-8")

    assert "mfl-classification-roster-bridge-batch-apply-evidence-" in workflow
    assert "verify_team_signal_delta_readback.py" in workflow
    assert "actions/cache/restore@v5" in workflow
    assert "actions/cache/save@v5" not in workflow
    assert "DELETE \"repos/${GITHUB_REPOSITORY}/actions/caches/" not in workflow
    assert "mfl_classification_roster_bridge_batch_readback.json" in workflow
