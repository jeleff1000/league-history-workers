from pathlib import Path


def test_exact_team_null_promotion_is_locked_to_current_600_row_candidate() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github" / "workflows" / "research_promote_exact_team_null_candidate.yml"
    ).read_text(encoding="utf-8")

    assert 'default: "31668497497"' in workflow
    assert 'EXPECTED_DELTA_ROWS: "600"' in workflow
    assert 'EXPECTED_DELTA_UNIQUE_KEYS: "600"' in workflow
    assert "'loss':600, 'tie':600" in workflow
    assert "'win':0" in workflow
    assert "'team_points':0" in workflow
    assert "'is_playoffs':0" in workflow
    assert "'champion':0" in workflow
    assert "test \"${#ids[@]}\" -eq 1" in workflow
    assert "new_lineage':False" in workflow
