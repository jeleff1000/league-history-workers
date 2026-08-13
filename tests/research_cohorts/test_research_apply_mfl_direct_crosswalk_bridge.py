from __future__ import annotations

from pathlib import Path


def test_mfl_direct_crosswalk_bridge_promotion_is_exact_and_same_key() -> None:
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_apply_mfl_direct_crosswalk_bridge.yml"
    ).read_text(encoding="utf-8")

    assert "CANDIDATE_RUN_ID: \"31737829461\"" in workflow
    assert "EXPECTED_DELTA_ROWS: \"14\"" in workflow
    assert "EXPECTED_WIN_FILLS: \"7\"" in workflow
    assert "EXPECTED_LOSS_FILLS: \"14\"" in workflow
    assert "EXPECTED_TIE_FILLS: \"14\"" in workflow
    assert "EXPECTED_TEAM_POINT_FILLS: \"7\"" in workflow
    assert "EXPECTED_PLAYOFF_FILLS: \"7\"" in workflow
    assert "apply_team_signal_delta_in_place.py" in workflow
    assert "verify_team_signal_delta_readback.py" in workflow
    assert "cmp -s cache_before.sha256 candidate_cache_before.sha256" in workflow
    assert "test \"${#ids[@]}\" -eq 1" in workflow
    assert "actions/cache/save@v5" in workflow
