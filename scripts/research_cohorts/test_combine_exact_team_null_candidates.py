import json

import duckdb
import pytest

from combine_exact_team_null_candidates import combine


def _candidate(root, name, source_loss=0):
    candidate = root / name
    candidate.mkdir()
    (candidate / "team_signal_candidate_report.json").write_text(json.dumps({
        "read_only": True,
        "cache_mutated": False,
        "new_lineage": False,
        "canonical_schema_unchanged": True,
    }))
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE delta AS
        SELECT 'league'::VARCHAR db_name, 2024::INTEGER year, 1::INTEGER week,
               'player'::VARCHAR NFL_player_id, 'manager'::VARCHAR manager,
               NULL::VARCHAR team_key, NULL::VARCHAR team_name, 'sleeper'::VARCHAR platform,
               1::INTEGER source_win, ?::INTEGER source_loss, 0::INTEGER source_tie,
               100.0::DOUBLE source_team_points, 0::INTEGER source_playoffs,
               NULL::INTEGER source_champion, NULL::INTEGER source_final_playoff_seed,
               1::INTEGER canonical_win, NULL::INTEGER canonical_loss,
               NULL::INTEGER canonical_tie, 100.0::DOUBLE canonical_team_points,
               0::INTEGER canonical_is_playoffs, 0::INTEGER canonical_champion,
               NULL::INTEGER canonical_final_playoff_seed
    """, [source_loss])
    con.execute("COPY delta TO ? (FORMAT PARQUET)", [str(candidate / "team_promotable_player_delta.parquet")])
    con.close()
    return candidate


def test_combine_deduplicates_agreeing_null_key_rows(tmp_path):
    roots = [_candidate(tmp_path, "first"), _candidate(tmp_path, "second")]
    delta = tmp_path / "combined.parquet"
    result = combine(roots, delta, tmp_path / "combined.json")
    assert result["raw_rows"] == 2
    assert result["unique_rows"] == 1
    assert result["conflicting_duplicate_keys"] == 0
    con = duckdb.connect()
    assert con.execute("SELECT source_win,source_loss,source_tie,source_team_points FROM read_parquet(?)", [str(delta)]).fetchone() == (None, 0, 0, None)
    con.close()


def test_combine_rejects_conflicting_loss_for_same_null_key(tmp_path):
    roots = [_candidate(tmp_path, "first", 0), _candidate(tmp_path, "second", 1)]
    with pytest.raises(SystemExit, match="conflicting duplicate source values"):
        combine(roots, tmp_path / "combined.parquet", tmp_path / "combined.json")
