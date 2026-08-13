from pathlib import Path


def test_readback_workflow_derives_ledger_guard_counts_from_the_materialized_input() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github" / "workflows" / "research_readback_team_signal_artifacts.yml"
    ).read_text(encoding="utf-8")

    assert "expected=json.loads(Path('ledger_receipt_input.json').read_text())['rows']" in workflow
    assert "receipt['summary']['ledger_rows'] == len(expected)" in workflow
    assert "receipt['summary']['raw_artifact_rows'] == sum(" in workflow
    assert "receipt['summary']['supplemental_cache_recovery_receipt_rows'] == sum(" in workflow
    assert "== 9480" not in workflow
    assert "== 1" not in workflow
    assert "- uses: actions/upload-artifact@v4\n        if: always()" in workflow
