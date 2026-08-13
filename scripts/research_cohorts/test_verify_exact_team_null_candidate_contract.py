import json

import duckdb
import pytest

from verify_exact_team_null_candidate_contract import verify


def test_verify_accepts_only_loss_tie_null_fill_candidate(tmp_path):
    """The promotion guard accepts a candidate that can change only loss/tie."""
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    report = {
        "read_only": True,
        "cache_mutated": False,
        "new_lineage": False,
        "canonical_schema_unchanged": True,
        "candidate_files": 1,
        "source_rows": 1,
        "source_team_keys": 1,
        "unmatched_source_team_keys": 0,
        "matched_player_rows": 1,
        "improvements_by_field": {
            "win": 0, "loss": 1, "tie": 1, "team_points": 0,
            "is_playoffs": 0, "champion": 0, "final_playoff_seed": 0,
            "made_playoffs": 0,
        },
    }
    (candidate / "team_signal_candidate_report.json").write_text(json.dumps(report))
    (candidate / "raw_source_manifest.json").write_text("[]\n")
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE d AS
        SELECT 'league'::VARCHAR AS db_name, 2024::INTEGER AS year, 1::INTEGER AS week,
               'nfl'::VARCHAR AS NFL_player_id, 'mgr'::VARCHAR AS manager,
               NULL::VARCHAR AS team_key, NULL::VARCHAR AS team_name,
               'sleeper'::VARCHAR AS platform,
               1::INTEGER AS source_win, 0::INTEGER AS source_loss,
               0::INTEGER AS source_tie, 100.0::DOUBLE AS source_team_points,
               0::INTEGER AS source_playoffs, NULL::INTEGER AS source_champion,
               NULL::INTEGER AS source_final_playoff_seed,
               1::INTEGER AS canonical_win, NULL::INTEGER AS canonical_loss,
               NULL::INTEGER AS canonical_tie, 100.0::DOUBLE AS canonical_team_points,
               0::INTEGER AS canonical_is_playoffs, 0::INTEGER AS canonical_champion,
               NULL::INTEGER AS canonical_final_playoff_seed,
               NULL::INTEGER AS canonical_made_playoffs
    """)
    con.execute("COPY d TO ? (FORMAT PARQUET)", [str(candidate / "team_promotable_player_delta.parquet")])
    con.close()

    result = verify(candidate)

    assert result["raw_rows"] == 1
    assert result["unique_rows"] == 1
    assert result["loss_null_rows"] == 1
    assert result["tie_null_rows"] == 1
    assert result["other_fill_rows"] == 0


def test_verify_rejects_conflicting_duplicate_loss_value(tmp_path):
    """The updater must never choose between duplicate source outcomes."""
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    (candidate / "team_signal_candidate_report.json").write_text(json.dumps({
        "read_only": True,
        "cache_mutated": False,
        "new_lineage": False,
        "canonical_schema_unchanged": True,
    }))
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE d AS
        SELECT 'league'::VARCHAR AS db_name, 2024::INTEGER AS year, 1::INTEGER AS week,
               'nfl'::VARCHAR AS NFL_player_id, 'mgr'::VARCHAR AS manager,
               NULL::VARCHAR AS team_key, NULL::VARCHAR AS team_name,
               'sleeper'::VARCHAR AS platform,
               1::INTEGER AS source_win, 0::INTEGER AS source_loss,
               0::INTEGER AS source_tie, 100.0::DOUBLE AS source_team_points,
               0::INTEGER AS source_playoffs, NULL::INTEGER AS source_champion,
               NULL::INTEGER AS source_final_playoff_seed,
               1::INTEGER AS canonical_win, NULL::INTEGER AS canonical_loss,
               NULL::INTEGER AS canonical_tie, 100.0::DOUBLE AS canonical_team_points,
               0::INTEGER AS canonical_is_playoffs, 0::INTEGER AS canonical_champion,
               NULL::INTEGER AS canonical_final_playoff_seed,
               NULL::INTEGER AS canonical_made_playoffs
    """)
    con.execute("INSERT INTO d SELECT * REPLACE (1::INTEGER AS source_loss) FROM d")
    con.execute("COPY d TO ? (FORMAT PARQUET)", [str(candidate / "team_promotable_player_delta.parquet")])
    con.close()

    with pytest.raises(SystemExit, match="conflicting duplicate source values"):
        verify(candidate)
