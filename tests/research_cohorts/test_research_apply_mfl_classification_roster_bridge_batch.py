from pathlib import Path


def test_batch_apply_rebuilds_candidates_from_every_completed_shard_without_new_lineage() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github" / "workflows" / "research_apply_mfl_classification_roster_bridge_batch.yml"
    ).read_text(encoding="utf-8")

    assert "gh api --paginate" in workflow
    assert "excluded_shards" in workflow
    assert "validate_mfl_shard_artifacts.py" in workflow
    assert "--excluded-shards \"$EXCLUDED_SHARDS\"" in workflow
    assert "team_signals.parquet" in workflow
    assert "player_bridge.parquet" in workflow
    assert "--emit-direct-mfl-win-replacements" in workflow
    assert "--replace-direct-mfl-win" in workflow
    assert "actions/cache/save@v5" in workflow
    assert "new_lineage" in workflow
    assert 'test "${#ids[@]}" -eq 1' in workflow
    assert 'DELETE "repos/${GITHUB_REPOSITORY}/actions/caches/${ids[0]}"' in workflow
    assert "mfl_classification_roster_bridge_batch_ledger_receipt.json" in workflow
    assert "promotion_disposition" in workflow
    assert "--pattern 'research-mfl-classification-roster-bridge-shard-*'" in workflow
